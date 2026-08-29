// 배치 실행 이력을 조회합니다.
package com.kafkick.api.admin.batch;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.springframework.beans.factory.ObjectProvider;

import com.kafkick.api.admin.batch.dto.BatchHistoryResponse;
import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.caller.Caller;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.batch.BatchExecution;
import com.kafkick.core.batch.BatchExecutionRepository;
import com.kafkick.core.support.exception.BusinessException;

import java.util.List;

/**
 * 배치가 <b>언제 무엇이 어떤 결과로</b> 끝났는지 본다.
 *
 * <p><b>"마지막 성공 이후 몇 초" 는 여기서 주지 않는다.</b> 그것은 1초 단위로 추적해야 하는
 * 값이라 Prometheus 의 {@code cy_batch_last_success_seconds}(feature/CY-15 소유) 로 가고, 화면이
 * {@code time() - 값} 으로 환산한다. 목록 조회로 그것을 만들면 폴링 주기가 곧 해상도가 된다.
 *
 * <p><b>조회는 관측 전용 풀로 나간다.</b> 어댑터
 * ({@code JdbcBatchExecutionRepository})가 {@code @Qualifier("obs")} 하나만 문다.
 * 부하 회차 중에 이 조회가 운영 풀의 커넥션을 물면 그 자체가 측정 오염이다.
 *
 * <p><b>관측을 끄면 503 과 {@code ADMIN-003} 으로 답한다.</b> 읽을 원천이 관측 풀 하나뿐이라
 * 어댑터({@code JdbcBatchExecutionRepository})가 그 스위치를 갖고 있다. 세 갈래 중에서 골랐다 —
 * 컨트롤러도 함께 사라지게 하면 <b>404</b> 라 "기능 없음" 과 안 갈리고, 빈 목록(200)이면
 * <b>"배치가 한 번도 안 돌았다"</b> 로 읽힌다. 503 은 "지금은 못 준다" 를 말하고 코드가
 * "왜" 를 말한다. {@code BatchHistoryDisabledHttpContractTest} 가 고정한다.
 *
 * <p><b>이 목록은 보존 기간까지만 거슬러 간다.</b> 정리 잡이 기본 30일 밖을 걷는다
 * ({@code batch.cleanup.metadata-keep-days}, 하한 8 · 상한 365 — {@code feature/CY-15} 소유).
 * {@code jobName} 을 걸어도 깊이는 같다. 다만 <b>{@code STARTED} 인 채 끝나지 않은 실행은
 * 지워지지 않아 아무리 오래돼도 남는다</b> — 그것이 목록 맨 위에 오는 것은 정상이고,
 * 사람이 복구해야 하는 신호다. 자세한 계약은 {@code BatchExecutionRepository} 에 적었다.
 *
 * <p><b>검증 배치의 판정({@code verification_runs})은 여기서 합치지 않는다.</b> 이유가 둘이다 —
 * <ul>
 *   <li>{@code verification_runs} 에는 {@code JOB_EXECUTION_ID} 가 없다. 이 목록과 짝지으려면
 *       {@code as_of}·{@code attempt} 로 추측해야 하고, 그 추측이 틀려도 화면에는
 *       <b>그럴듯한 값</b>이 뜬다</li>
 *   <li>그 목록은 이미 {@code AdminVerificationController} 의 {@code GET /verification-runs} 가
 *       소유한다. 여기서 또 만들면 같은 사실을 두 경로가 각자 읽는다</li>
 * </ul>
 * {@code verifyJob} 이 <b>언제 돌았고 성공했는지</b>는 이 목록에 그대로 나온다 —
 * Spring Batch 잡이기 때문이다. 여기 없는 것은 그 실행의 <b>판정 결과</b>뿐이다.
 */
@RestController
@RequestMapping("/api/v1/admin")
public class BatchHistoryController {

    /**
     * <b>{@code ObjectProvider} 로 받는 이유.</b> 어댑터는 관측 풀에만 기대므로
     * {@code observation.datasource.enabled} 가 꺼지면 빈이 없다. 생성자로 직접 받으면
     * 그 환경에서 <b>api 기동이 통째로 실패</b>하고(실제로 {@code KafkaLayerWiringTest} 가
     * 그렇게 깨졌다), 컨트롤러에 같은 조건을 달면 경로가 <b>404</b> 가 되어
     * "기능 없음" 과 구분되지 않는다. 늦게 받아 <b>응답으로 설명</b>한다.
     */
    private final ObjectProvider<BatchExecutionRepository> repository;

    public BatchHistoryController(ObjectProvider<BatchExecutionRepository> repository) {
        this.repository = repository;
    }

    /**
     * 배치 실행 이력을 최신부터 조회합니다.
     *
     * @param jobName 특정 잡만 볼 때 지정합니다. 비우면 전체입니다
     * @param limit 최대 건수. 기본 50, 허용 1~200
     * @param caller 요청자 식별용. <b>권한 판정에 쓰지 않는다</b>({@code Caller} javadoc) —
     *        이 파라미터가 하는 일은 {@code X-User-Id} 를 <b>필수로 만드는 것</b>이다
     *        ({@code CallerArgumentResolver} 가 없으면 400 을 던진다. {@code Optional<Caller>}
     *        로 선언하면 없어도 통과한다). 관리자 역할 확인은 이 메서드 밖에서 —
     *        {@code AdminWebConfig} 가 {@code /api/v1/admin/**} 에 건
     *        {@code AdminAuthorizationInterceptor} 가 {@code X-User-Role: ADMIN} 이 아니면
     *        403 으로 끊는다
     * @return 실행 이력 목록
     */
    @GetMapping("/batch-executions")
    public ResponseEnvelope<BatchHistoryResponse> executions(
            @RequestParam(required = false) String jobName,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) Integer limit,
            Caller caller) {
        BatchExecutionRepository source = repository.getIfAvailable();
        if (source == null) {
            throw new BusinessException(AdminApiErrorCode.OBSERVATION_DISABLED);
        }
        List<BatchExecution> executions = jobName == null || jobName.isBlank()
                ? source.findRecent(limit)
                : source.findRecentByJobName(jobName, limit);
        return ResponseEnvelope.success(BatchHistoryResponse.of(executions));
    }
}
