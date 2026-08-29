package com.kafkick.batch.api;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

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
 */
public record BatchRunView(
        long executionId,
        String jobName,
        String status,
        String failure,
        LocalDateTime startedAt,
        LocalDateTime finishedAt,
        Long durationSeconds,
        Long readCount,
        Long writeCount) {

    public static BatchRunView of(BatchRun run) {
        return of(run, ZoneId.systemDefault());
    }

    /**
     * 존을 인자로 받는 갈래. 기본 존은 테스트가 못 바꾸므로 변환 자체를 재려면 이쪽이
     * 필요하다 — 통합 테스트는 build.gradle 의 user.timezone 한 줄에 매여 그 값이 UTC 가
     * 되는 날 통째로 건너뛰어진다. BatchTimeAxis 가 같은 이유로 같은 갈래를 연다.
     */
    static BatchRunView of(BatchRun run, ZoneId batchMetaZone) {
        LocalDateTime startedAt = onDomainAxis(run.startedAt(), batchMetaZone);
        LocalDateTime finishedAt = onDomainAxis(run.finishedAt(), batchMetaZone);
        return new BatchRunView(
                run.executionId(),
                run.jobName(),
                run.status(),
                FailureSummary.of(run.exitMessage()),
                startedAt,
                finishedAt,
                durationSeconds(startedAt, finishedAt),
                run.readCount(),
                run.writeCount());
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
