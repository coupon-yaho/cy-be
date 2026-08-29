package com.kafkick.batch.api;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Set;

import com.kafkick.batch.config.BatchTimeAxis;
import com.kafkick.core.batch.BatchRun;

/**
 * 배치 실행 이력 한 줄. 관제 화면의 히스토리 표가 그대로 쓴다.
 *
 * <p>exitCode 는 안 싣는다. SimpleJob 이 잡 종료 코드를 마지막 Step 값으로 대입하는데,
 * statsAggregateStep 이 CORRUPT 에서 항상 "SKIPPED" 를 세운다 — 정상적으로 끝난 오염셋
 * 검증이 STATUS=COMPLETED, EXIT_CODE=SKIPPED 로 남는다(실측). 이 표는 verifyJob 행을
 * 그대로 포함하므로, 그 값을 노출하면 화면이 상태 배지를 그것으로 칠하고 성공한 검증이
 * 스킵으로 읽힌다. 성공은 status 와 failure 로 본다. VerifyRunView 가 같은 근거로 뺐다.
 *
 * <p>⚠️ <b>{@code stepReadTotal}·{@code stepWriteTotal} 을 "처리 건수" 로 칠하지 마라.</b>
 * 그 실행의 <b>모든 Step 의 {@code READ_COUNT}·{@code WRITE_COUNT} 를 그냥 더한 값</b>이고,
 * 잡마다 — 심지어 한 잡 안에서도 — <b>세는 단위가 다르다.</b> 실측(300만 발급 · 534만 이력):
 *
 * <pre>
 *   verifyJob   replayStep        read 3,000,000 / write 3,000,000  (리플레이 상태 행)
 *               statsAggregateStep             write       783      (통계 스냅샷 행)
 *               나머지 아홉                     0 / 0               (태스클릿이라 안 센다)
 *               → 합계 write 3,000,783 은 <b>리플레이 행 + 통계 행</b>이다
 *   expireJob   청크가 후보를 읽고 만료 행을 쓴다 — 여기서는 뜻이 맞는다
 *   cleanupJob  Step 둘이 <b>서로 다른 표의 삭제 행</b>을 센다
 * </pre>
 *
 * <p>그래서 이름을 {@code readCount}/{@code writeCount} 가 아니라 <b>{@code step*Total}</b> 로
 * 둔다 — 한때 앞엣것이었고 <b>도메인 처리 건수로 읽히는 것이 정확히 그 사고</b>다.
 * 화면에는 <i>"Step 처리 합계"</i> 처럼 출처가 드러나게 적고, 검출 건수나 만료 건수가
 * 필요하면 그 잡의 전용 응답을 봐라 — 검증은 {@code VerifyRunView.findingCount} 다.
 * <b>안 지우는 이유</b>는 만료·정리에서는 이 값이 "그 실행이 몇 행을 만졌나" 로 여전히
 * 쓸모가 있고, 지우면 관제에서 그 축이 통째로 사라지기 때문이다.
 */
public record BatchRunView(
        long executionId,
        String jobName,
        String status,
        String failure,
        LocalDateTime startedAt,
        LocalDateTime finishedAt,
        Long durationSeconds,
        Long stepReadTotal,
        Long stepWriteTotal) {
    /**
     * <b>사유를 안 붙이는 상태.</b> 끝나서 성공한 것 하나와, <b>아직 안 끝난</b> 셋이다 —
     * 도는 중에 사유를 찍으면 화면이 "실패했는데 안 끝났다" 로 읽는다.
     *
     * <p><b>BatchStatus 전수 실측</b>(6.0.4):
     * <pre>
     * COMPLETED  isRunning=false  isUnsuccessful=false   성공
     * STARTING   isRunning=true   isUnsuccessful=false   아직
     * STARTED    isRunning=true   isUnsuccessful=false   아직
     * STOPPING   isRunning=true   isUnsuccessful=false   아직
     * STOPPED    isRunning=false  isUnsuccessful=false   ★ 끝났는데 성공이 아니다
     * FAILED     isRunning=false  isUnsuccessful=true
     * ABANDONED  isRunning=false  isUnsuccessful=true
     * UNKNOWN    isRunning=false  isUnsuccessful=true
     * </pre>
     *
     * <p><b>널 검사를 먼저 한다.</b> {@code Set.of(...).contains(null)} 은 false 가 아니라
     * <b>NPE 를 던진다</b>(실측). {@code STATUS} 는 스프링 배치 원본 스키마 그대로라
     * {@code NOT NULL} 이 아니고({@code V11__batch_metadata.sql}), 이 목록은 이상한 행도
     * 보여 주는 자리다 — 한 행 때문에 던지면 <b>목록 전체가 500</b> 이 된다. 아래 시각
     * 널가드와 같은 근거다. NULL 은 <b>모르는 상태</b>라 실패 쪽으로 접는다.
     *
     * <p>★ 표가 {@code isUnsuccessful()} 을 못 쓰는 두 번째 이유다. 그것으로 가르면
     * <b>운영자가 세운 실행이 완주한 실행과 응답에서 똑같이</b> 보인다 — 이 저장소는
     * {@code VerifyStopService} 로 도는 검증을 세우는 경로를 실제로 열어 뒀다.
     */
    private static final Set<String> SUCCEEDED =
            Set.of("COMPLETED", "STARTING", "STARTED", "STOPPING");


    public static BatchRunView of(BatchRun run) {
        return of(run, ZoneId.systemDefault());
    }

    /**
     * 존을 인자로 받는 갈래. 기본 존은 테스트가 못 바꾸므로 변환 자체를 재려면 이쪽이
     * 필요하다 — 통합 테스트는 build.gradle 의 user.timezone 한 줄에 매여 그 값이 UTC 가
     * 되는 날 통째로 건너뛰어진다. BatchTimeAxis 가 같은 이유로 같은 갈래를 연다.
     */
    static BatchRunView of(BatchRun run, ZoneId batchMetaZone) {
        LocalDateTime startedAt = onDomainAxis(run.startedAtInBatchMetaZone(), batchMetaZone);
        LocalDateTime finishedAt = onDomainAxis(run.finishedAtInBatchMetaZone(), batchMetaZone);
        return new BatchRunView(
                run.executionId(),
                run.jobName(),
                run.status(),
                failureOf(run),
                startedAt,
                finishedAt,
                durationSeconds(startedAt, finishedAt),
                run.stepReadTotal(),
                run.stepWriteTotal());
    }

    /**
     * 실패한 실행만 요약한다.
     *
     * <p>EXIT_MESSAGE 는 성공 실행에도 채워진다 — SimpleJob 이 마지막 Step 의 ExitStatus 를
     * 통째로 잡에 대입하고(6.0.4 바이트코드), statsAggregateStep 은 성공 시
     * "회차 147 · 등급쌍 468 · …" 를 설명으로 세운다(실측). 그것을 요약기에 넣으면 도메인
     * 코드도 예외 이름도 없어 "알 수 없는 오류" 로 접힌다 — 정상 종료한 모든 행에 실패
     * 사유가 찍힌다. exitCode 를 뺀 이유와 같은 뿌리다: 그 둘은 같은 ExitStatus 의 양쪽이다.
     *
     * <p>⚠️ BatchStatus.match 를 쓰지 않는다. 그것은 모르는 문자열을 UNKNOWN 이 아니라
     * <b>COMPLETED</b> 로 접는다(실측: "무엇인가"·""·"X" 가 전부 COMPLETED). 그러면 규약 밖
     * 값이 조용히 성공이 되어 실패 사유가 사라진다. 아는 성공 상태만 열거하고 나머지는
     * 실패로 본다 — 모르는 것을 성공이라고 말하는 쪽이 더 나쁘다.
     */
    private static String failureOf(BatchRun run) {
        return run.status() != null && SUCCEEDED.contains(run.status())
                ? null
                : FailureSummary.of(run.exitMessage());
    }

    /**
     * 배치 메타 시각은 JVM 기본 존이라 도메인 축(UTC)으로 옮긴다. 안 옮기면 같은 응답 안의
     * verification 시각과 좌표계가 갈린다 — CY-743 과 같은 이유다.
     *
     * <p>실행기가 거절한 행은 START_TIME 이 비어 있다. 이 목록은 그런 행도 보여 주는 자리라
     * 여기서 던지면 목록 전체가 500 이 된다.
     */
    private static LocalDateTime onDomainAxis(LocalDateTime batchMetaTime, ZoneId zone) {
        return batchMetaTime == null ? null : BatchTimeAxis.onDomainAxis(batchMetaTime, zone);
    }

    /** 도는 중이거나 시작조차 못 한 실행은 소요를 못 낸다. */
    private static Long durationSeconds(LocalDateTime startedAt, LocalDateTime finishedAt) {
        if (startedAt == null || finishedAt == null) {
            return null;
        }
        return Duration.between(startedAt, finishedAt).toSeconds();
    }
}
