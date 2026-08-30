// 검증 판정을 읽는 조회 API 입니다. 아무것도 쓰지 않습니다.
package com.kafkick.batch.api;

import java.util.List;
import java.time.Duration;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.response.ResponseEnvelope;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ExpectedFindingRepository;
import com.kafkick.core.verification.FindingKey;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationFindingRepository;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.VerificationRunRepository;
import com.kafkick.core.verification.exception.VerificationErrorCode;

/**
 * <b>D13 제출물을 뜨는 자리다.</b> {@code docs/10} 이 리포트를 <i>"두 얼굴"</i> 로 갈랐고,
 * 이쪽이 <b>제출용 — 최종 run 의 요약</b>이다.
 *
 * <h2>왜 {@code VerifyTriggerController} 에 안 붙였나</h2>
 *
 * <p>그쪽 생성자가 이미 여덟을 받는다 — 실행기·잡·저장소·시계·프로브에 <b>만료 크론과
 * 스케줄링 플래그</b>까지. 전부 <i>트리거</i>를 위한 것이다. 여기에 저장소 셋을 더하면
 * 그 컨트롤러가 <b>자기가 안 쓰는 것을 지게 된다.</b>
 *
 * <p>README 의 패키지 표가 {@code api/} 를 <i>"verify 트리거·조회"</i> 로 적어 뒀으므로
 * 자리는 여기가 맞다. 가르는 것은 패키지가 아니라 <b>클래스</b>다.
 *
 * <h2>읽기 전용이지만 트랜잭션은 건다</h2>
 *
 * <p><b>한때 여기 "조회뿐이라 트랜잭션 경계가 없다" 고 적혀 있었는데, 그것이 결함이었다.</b>
 * 경계가 없으면 다섯 SELECT 가 autocommit 으로 각자 자기 스냅샷을 연다. 그 사이에
 * {@code expected_findings} 가 바뀌면 <b>한 응답 안에서 앞뒤가 다른 것을 대조한 결과</b>가
 * 나온다 — {@code missing} 은 옛 정답 기준, {@code unexpected} 는 새 정답 기준이 되어
 * <i>"검증기가 800건을 오탐했다"</i> 가 {@code verdict=PASS} 옆에 실린다.
 *
 * <p>그래서 {@code latest()} 에 {@code @Transactional(readOnly = true)} 를 건다. 이 DB 는
 * {@code REPEATABLE-READ}(실측) 라 다섯 읽기가 <b>한 스냅샷</b>에 묶인다. 쓰기가 없어
 * 부작용도 없다.
 *
 * <p><b>정리 잡은 이 응답을 안 흔든다.</b> {@code CleanupJdbcAdapter} 의 첫 문단이
 * <i>"{@code verification_runs} 행은 안 지운다"</i> 고 못 박는다.
 *
 * <p>검출 행을 지우는 갈래는 {@code verdict IS NULL} 이거나 {@code CLEAN·PASS} 인데,
 * 이 조회가 고르는 것은 {@code verdict IS NOT NULL} 이고 {@code CLEAN·PASS} 는 정의상
 * 검출 0행이다. {@code expected_findings} 는 아예 정리 대상이 아니다.
 *
 * <p><b>흔들리는 축은 따로 있다 — 시드 재주입이다.</b> 아래 {@code manifest} 가
 * {@code exists} 로 <i>"정답 묶음이 통째로 사라진"</i> 경우를 거르고, 위 트랜잭션이
 * <i>"읽는 도중에 바뀌는"</i> 경우를 막는다. <b>둘 다 있어야 한다</b> — {@code exists} 는
 * 한 순간만 보므로 그 뒤의 변경을 못 막고, 스냅샷은 조회 시작 전에 이미 사라진 것을
 * 되살리지 못한다.
 */
@RestController
@RequestMapping("/api/v1/admin/verify")
public class VerifyReportController {

    private final VerificationRunRepository runs;
    private final VerificationFindingRepository findings;
    private final ExpectedFindingRepository expected;

    /** 지금 시각. 판정 없는 실행이 "아직 도는 중" 인지 "죽은 것" 인지 나이로 가르는 데 쓴다. */
    private final TimeProvider timeProvider;

    /**
     * 이 시간이 지나도록 판정이 안 나면 {@code STALE} 로 답한다.
     * {@code RunningJobProbe} 가 "멈춘 잡" 을 보는 값과 <b>같은 손잡이</b>를 쓴다 —
     * 둘이 갈리면 같은 실행을 한 화면은 살아 있다고, 다른 화면은 멈췄다고 말한다.
     */
    private final Duration stuckAfter;

    public VerifyReportController(VerificationRunRepository runs,
            VerificationFindingRepository findings,
            ExpectedFindingRepository expected,
            TimeProvider timeProvider,
            @Value("${batch.stuck-job-after-ms:1800000}") long stuckAfterMs) {
        this.runs = runs;
        this.findings = findings;
        this.expected = expected;
        this.timeProvider = timeProvider;
        this.stuckAfter = Duration.ofMillis(stuckAfterMs);
    }

    /**
     * 이 조합의 <b>마지막으로 닫힌</b> 실행을 리포트로 준다.
     *
     * <p><b>{@code runId} 가 아니라 조합으로 찾는 것이 결정이다.</b> 제출물이 필요로 하는 것은
     * <i>"최종 결과"</i> 이고, 그것을 {@code runId} 로 받으면 운영자가 그 번호를 먼저 찾아야
     * 하는데 <b>실행 목록을 주는 조회가 없다.</b> 그리고 {@code findLatestClosed} 는
     * 지표가 이미 쓰는 포트다 — 같은 것을 두 번 만들지 않는다.
     *
     * <p><b>닫힌 것만 온다.</b> {@code finished_at} 이 없는 행은 돌다 말았거나 지금 도는
     * 중이라 판정이 아니다. 그것을 제출물에 실으면 <i>"판정이 없다"</i> 가
     * <i>"아직 안 끝났다"</i> 와 한 값으로 뭉친다.
     *
     * <pre>
     * curl -sSf "localhost:9091/api/v1/admin/verify/reports/latest?dataset=CLEAN&amp;scope=FULL" \
     *   &gt; verify-clean-full.json
     * </pre>
     */
    @GetMapping("/reports/latest")
    // 데드라인도 함께 준다(CY-697). readOnly 만으로는 끊을 수단이 없다 — 다섯 SELECT 중
    // DB 가 멈추면 이 요청만 톰캣 스레드를 무기한 물고, 새 503 갈래도 안 탄다.
    @org.springframework.transaction.annotation.Transactional(readOnly = true,
            timeoutString = "${batch.admin.timeout-seconds:5}")
    public ResponseEnvelope<VerifyReportView> latest(
            @RequestParam DatasetType dataset,
            @RequestParam ScopeType scope) {

        VerificationRun run = runs.findLatestClosed(dataset, scope)
                .orElseThrow(() -> new BusinessException(VerificationErrorCode.RUN_NOT_FOUND,
                        "dataset=" + dataset + " scope=" + scope));

        return ResponseEnvelope.success(VerifyReportView.of(
                run, byType(run), manifest(run)));
    }

    /**
     * <b>진행 중인 실행의 중간 상태.</b> 화면이 "지금 검증" 을 누르고 결과를 기다리는 동안
     * 무엇이 몇 건 잡혔는지 보여 주려고 연다.
     *
     * <p><b>{@code /reports/latest} 를 완화하지 않는다.</b> 그쪽은 {@code verdict IS NOT NULL
     * AND finished_at IS NOT NULL} 로 <b>일부러</b> 진행 중 실행을 뺀다 — 게이트와
     * {@code dump-verify-report.sh} 가 같은 질의를 쓰므로, 거기에 진행 중 run 이 섞이면
     * 절반만 센 검출이 <i>최종 판정</i>으로 증적에 남는다. 그래서 <b>다른 경로·다른 타입</b>
     * 으로 가른다.
     *
     * <p>검출 수는 {@code verification_findings} 를 직접 센다. {@code verification_runs}
     * 의 {@code finding_count} 는 판정 Step 이 마감할 때 채워지므로 진행 중에는 안 맞는다.
     *
     * <p><b>없는 실행은 404 다</b> — {@code VERIFICATION-003}({@code RUN_NOT_FOUND})을
     * 봉투에 실어 보낸다. 화면이 트리거 응답의 {@code executionId}(배치 잡 실행)를 그대로
     * 넣으면 이 경로를 탄다 — 여기가 받는 것은 {@code runId}(검증 실행)이고 둘은 다른
     * 번호다. 실측에서 {@code executionId=15} 일 때 {@code runId=17} 이었다.
     *
     * <pre>
     * curl -sSf "localhost:9091/api/v1/admin/verify/runs/16/progress" -H "X-Batch-Admin-Token: …"
     * </pre>
     */
    @GetMapping("/runs/{runId}/progress")
    @org.springframework.transaction.annotation.Transactional(readOnly = true,
            timeoutString = "${batch.admin.timeout-seconds:5}")
    public ResponseEnvelope<VerifyProgressView> progress(@PathVariable long runId) {

        VerificationRun run = runs.findById(runId)
                .orElseThrow(() -> new BusinessException(VerificationErrorCode.RUN_NOT_FOUND,
                        "runId=" + runId));

        return ResponseEnvelope.success(VerifyProgressView.of(
                run, findings.countOf(runId), findings.countByType(runId),
                timeProvider.now(), stuckAfter));
    }

    private Map<FindingType, Integer> byType(VerificationRun run) {
        return findings.countByType(run.id());
    }

    /**
     * <b>오염셋이면서 대조를 실제로 한 실행에만 붙는다.</b>
     *
     * <p>{@code seed_run_id} 가 비어 있는 오염셋 실행이 있을 수 있다 — 대조 Step 까지 못 간
     * 실행이다. 그때 대조를 <b>빈 결과로 채우면 "일치했다" 로 읽힌다.</b> 없는 것은 없는
     * 채로 둔다({@code null}). 정상셋과는 같은 응답의 {@code run.dataset} 으로 갈린다.
     *
     * <p><b>정답 묶음이 사라진 경우를 먼저 거른다.</b> {@code expected_findings} 가 0행이면
     * {@code LEFT JOIN} 이 <b>검출 전부를 오탐으로 뒤집는다</b> —
     * {@code ExpectedFindingRepository.unexpected} 의 javadoc 이 <i>"그 경우를 판정 전에
     * 걸러야 '오탐 800' 이라는 엉뚱한 결론이 안 나온다"</i> 고 적어 뒀다. 시드가
     * {@code seed_run_id} 를 재사용·재주입할 수 있고 그 컬럼에 FK 가 없어서, 포인터만 남고
     * 대상이 사라지는 상태가 실제로 생긴다.
     *
     * <p><b>목록을 그대로 싣지 않는다.</b> 두 조회는 대조 결과 <b>전부</b>를 주고, 불일치가
     * 크면 그것이 수천 행이 된다 — 이 응답은 매일 공개 저장소에 커밋된다. 총수는 세고
     * 목록은 {@link VerifyReportView.Manifest#SAMPLE_LIMIT} 개까지만 싣는다.
     */
    private VerifyReportView.Manifest manifest(VerificationRun run) {
        if (run.dataset() != DatasetType.CORRUPT || run.seedRunId() == null) {
            return null;
        }

        long seedRunId = run.seedRunId();
        if (!expected.exists(seedRunId)) {
            return VerifyReportView.Manifest.absent(seedRunId);
        }

        List<FindingKey> missing = expected.missing(run.id(), seedRunId);
        List<FindingKey> unexpected = expected.unexpected(run.id(), seedRunId);

        // 자르는 것은 Manifest 가 한다. 여기서 자르면 총수와 목록이 어긋날 수 있다.
        return VerifyReportView.Manifest.compared(seedRunId,
                expected.countOf(seedRunId), expected.digestOf(seedRunId),
                missing, unexpected);
    }
}
