package com.kafkick.batch.config;

import com.kafkick.core.expiration.exception.ExpirationErrorCode;
import com.kafkick.core.support.exception.BusinessException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.stereotype.Component;

/**
 * <b>만료가 왜 실패했는지를 라벨로 가른다.</b> {@code docs/13} §2d 가 요구한 것이다.
 *
 * <h2>왜 새 지표인가</h2>
 *
 * <p>Micrometer 가 자동 등록하는 {@code spring_batch_job_seconds_count} 에는
 * <b>에러코드 라벨이 없고, 붙일 수도 없다</b> — 그 미터는 Spring Batch 가 만들고
 * 태그 집합이 고정이다. 그래서 만료의 실패 자리 다섯이 <b>한 시계열로 뭉쳐</b> 나오고,
 * 어느 자리였는지는 배치 로그의 {@code EXPIRATION-00N} 으로만 갈렸다.
 *
 * <p><b>각각 봐야 할 곳이 다르다.</b> 셋은 코드({@code EXPIRE_HISTORY_COUNT_MISMATCH} ·
 * {@code STOCK_ROW_MISSING} · {@code STOCK_UNDERFLOW}), 하나는 넘긴 파라미터
 * ({@code EXPIRE_ASOF_IN_FUTURE}), 하나는 접속 URL({@code EXPIRE_ON_CORRUPT_SCHEMA})이다.
 * 라벨이 없으면 알림을 받은 사람이 그 다섯을 <b>전부 확인해야</b> 한다.
 *
 * <h2>어디서 코드를 꺼내나</h2>
 *
 * <p>{@link JobExecution#getAllFailureExceptions()} 다. 잡·스텝 어느 층에서 던졌든 여기 모인다.
 * {@link BusinessException} 이면 그 {@code ErrorCode} 를, 아니면 {@code UNCLASSIFIED} 로 센다.
 *
 * <p><b>{@code UNCLASSIFIED} 가 0 이 아니면 그 자체가 신호다.</b> 만료 경로의 실패는 전부
 * {@code BusinessException} 이어야 한다 — 아니라는 것은 우리가 안 세던 자리가 생겼다는 뜻이다.
 *
 * <h2>라벨 카디널리티</h2>
 *
 * <p><b>{@code ErrorCode.getCode()} 만 라벨로 쓴다.</b> 값 집합이 열거형이라 유한하고
 * (잡 실패 다섯 + {@code UNCLASSIFIED}) 새 코드를 더하지 않는 한 안 는다.
 * 메시지나 {@code detail} 은 라벨에 넣지 않는다 — 회차 id 가 섞여 들어와 시계열이 폭발하고,
 * {@code detail} 은 로그용이라 PII 가 들어갈 수 있다({@code PRD:2143}).
 */
@Component
public class ExpireFailureMetrics implements JobExecutionListener {

    /** {@link BusinessException} 이 아닌 실패. 가드·파라미터 누락·DB 예외 셋 중 하나다. */
    static final String UNCLASSIFIED = "UNCLASSIFIED";

    private static final String METRIC = "cy_expire_failures_total";

    private static final Logger log = LoggerFactory.getLogger(ExpireFailureMetrics.class);

    private final MeterRegistry registry;

    /**
     * <b>실패하기 전에 전부 0 으로 등록한다.</b> 이유가 둘이다.
     *
     * <p>⑴ <b>{@code increase()} 가 첫 실패를 놓친다.</b> 시계열이 실패 순간에 처음 생기면
     * 그 창 안에 표본이 하나뿐이라 Prometheus 가 증가분을 못 낸다 — <b>첫 실패가 조용히
     * 넘어가고</b> 두 번째부터 울린다. 미리 0 이 찍혀 있으면 1 로 오르는 것이 증가로 잡힌다.
     *
     * <p>⑵ <b>{@code BatchMetricExposureTest} 가 규칙 파일과 노출을 잇는다.</b> 규칙이
     * 쓰는 이름이 실제로 안 나오면 그 알림은 <b>영원히 안 울리는데 아무도 모른다.</b>
     * 지연 등록이면 그 테스트가 잡을 수 없다.
     *
     * <p>카디널리티는 <b>여섯</b>이다(잡 실패 다섯 + {@code UNCLASSIFIED}).
     * 열거형이라 새 코드를 더하지 않는 한 안 는다.
     */
    public ExpireFailureMetrics(MeterRegistry registry) {
        this.registry = registry;
        for (ExpirationErrorCode code : ExpirationErrorCode.values()) {
            // **006·007 은 뺀다.** 그 둘은 ExpireAdminController(CY-429)의 거절 사유이지
            // 잡 실패가 아니다 — 열거형의 클래스 주석이 *"잡이 낼 수 있는 코드를 순회하는
            // 쪽(예: 지표 라벨)은 그 둘을 빼야 한다"* 고 명시했다. 넣으면 대시보드에
            // 만료와 무관한 라벨 둘이 영원히 0 으로 남고, 그것을 본 사람이 없는 runbook 을
            // 찾으러 간다(006 은 HTTP 404, 007 은 409 다).
            if (code.isJobFailure()) {
                counter(code.getCode());
            }
        }
        counter(UNCLASSIFIED);
    }

    private Counter counter(String errorCode) {
        return Counter.builder(METRIC)
                .description("만료 배치 실패 횟수 — 에러코드로 가른다")
                .tag("error_code", errorCode)
                .register(registry);
    }

    /**
     * <b>실패한 실행에서만 센다.</b> 성공 실행에도 예외가 남아 있을 수 있다 — 재시도가
     * 삼킨 것이다. 그것까지 세면 <i>"실패했다"</i> 는 뜻이 흐려진다.
     *
     * <p><b>예외 하나당 한 번 센다.</b> 한 실행이 여러 자리에서 죽으면 각각 라벨이 붙는다.
     * 그 편이 <i>"둘 중 무엇이 먼저였나"</i> 를 지우는 것보다 낫다.
     *
     * <p><b>여기서 예외를 던지지 않는다.</b> 다만 이유가 <i>"실행 결과가 덮인다"</i> 는
     * 아니다 — 한때 그렇게 적었는데 실측이 반대다. Spring Batch 6.0.4 의
     * {@code AbstractJob.execute} 는 {@code afterJob} 호출을 {@code catch (Exception)} 으로
     * 감싸 <b>로그만 남긴다</b>(javap 로 확인: <i>"Exception encountered in afterJob callback"</i>).
     *
     * <p><b>실제로 잃는 것은 뒤 리스너다.</b> {@code CompositeJobExecutionListener} 는
     * 리스너별로 안 잡고 <b>역순</b>으로 돈다. 등록이
     * {@code binlogFormatGuard → 이것 → cleanSchemaGuard} 이므로 여기서 던지면
     * {@code BinlogFormatGuard.afterJob} 이 통째로 건너뛰어진다.
     */
    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus().isUnsuccessful()) {
            try {
                for (Throwable cause : jobExecution.getAllFailureExceptions()) {
                    counter(codeOf(cause)).increment();
                }
            } catch (RuntimeException e) {
                log.warn("만료 실패 지표를 남기지 못했습니다. 원인 판정에는 영향이 없습니다.", e);
            }
        }
    }

    /**
     * <b>원인 사슬을 따라간다.</b> Spring Batch 가 {@code BusinessException} 을
     * {@code UncategorizedSQLException} 같은 것으로 감싸는 자리가 있어, 맨 위만 보면
     * 우리가 낸 코드가 전부 {@code UNCLASSIFIED} 로 샌다.
     *
     * <p>사슬이 자기를 참조하는 경우(직접 만들면 가능하다)를 대비해 깊이를 막는다.
     */
    private static String codeOf(Throwable cause) {
        int depth = 0;
        for (Throwable t = cause; t != null && depth < 16; t = t.getCause(), depth++) {
            if (t instanceof BusinessException business) {
                return business.getErrorCode().getCode();
            }
        }
        return UNCLASSIFIED;
    }
}
