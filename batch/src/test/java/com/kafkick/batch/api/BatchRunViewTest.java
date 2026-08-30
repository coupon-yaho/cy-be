package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneId;

import org.junit.jupiter.api.DisplayName;
import org.springframework.batch.core.BatchStatus;
import org.junit.jupiter.api.Test;

import com.kafkick.core.batch.BatchRun;

/**
 * 배치 이력 응답 모델의 변환.
 *
 * <p>DB 없이 잰다. 여기서 재려는 것은 저장도 컨트롤러도 아니라 이 변환이다 —
 * 엔드포인트 자체는 BatchHistoryApiTest 가 HTTP 로 잰다.
 */
class BatchRunViewTest {

    /**
     * 존을 인자로 고정한다. assumeThat 으로 두면 build.gradle 의 user.timezone 이 UTC 로
     * 바뀌는 날 이 검증이 실패가 아니라 침묵으로 사라진다 — skip 은 리포트에서 안 보인다.
     */
    @Test
    @DisplayName("배치 메타 시각을 도메인 축으로 옮겨 내보낸다")
    void movesBatchMetaTimesOntoTheDomainAxis() {
        ZoneId seoul = ZoneId.of("Asia/Seoul");
        LocalDateTime jvmWall = LocalDateTime.of(2026, 1, 15, 18, 30, 7);
        LocalDateTime utcWall = LocalDateTime.of(2026, 1, 15, 9, 30, 7);

        BatchRunView view = BatchRunView.of(run(jvmWall, jvmWall.plusSeconds(12)), seoul);

        assertThat(view.startedAt())
                .as("안 옮기면 같은 화면의 검증 이력(UTC)과 좌표계가 갈린다")
                .isEqualTo(utcWall)
                .isNotEqualTo(jvmWall);
        assertThat(view.finishedAt()).isEqualTo(utcWall.plusSeconds(12));
    }

    @Test
    @DisplayName("소요는 두 시각의 차이다 — 축을 옮겨도 안 바뀐다")
    void reportsDurationInSeconds() {
        LocalDateTime startedAt = LocalDateTime.of(2026, 1, 15, 18, 30, 7);

        assertThat(BatchRunView.of(run(startedAt, startedAt.plusSeconds(12))).durationSeconds())
                .isEqualTo(12L);
    }

    /**
     * 실행기가 거절한 행은 START_TIME 이 비어 있다. 이 목록은 그런 행도 보여 주는 자리라
     * 여기서 던지면 목록 전체가 500 이 된다.
     */
    @Test
    @DisplayName("시작조차 못 한 실행도 안 죽고 나간다")
    void survivesMissingTimes() {
        BatchRunView view = BatchRunView.of(run(null, null));

        assertThat(view.startedAt()).isNull();
        assertThat(view.finishedAt()).isNull();
        assertThat(view.durationSeconds())
                .as("소요를 0 으로 내면 '즉시 끝났다' 로 읽힌다 — 모르는 것은 비운다")
                .isNull();
    }

    @Test
    @DisplayName("도는 중이면 소요가 비어 있다")
    void hasNoDurationWhileRunning() {
        assertThat(BatchRunView.of(run(LocalDateTime.of(2026, 1, 15, 18, 30), null))
                .durationSeconds()).isNull();
    }

    /**
     * EXIT_MESSAGE 에는 스택트레이스가 통째로 들어간다(실측: 2,178자). 관문(CY-742)은 있지만 TLS 가 없고 토큰 회수 수단도 없어,
     * 그대로 실으면 내부 구조가 밖으로 나간다.
     */
    @Test
    @DisplayName("실패 원인은 요약만 나간다 — 스택트레이스가 안 샌다")
    void doesNotLeakTheStackTrace() {
        String realExitMessage = "com.kafkick.core.support.exception.BusinessException: "
                + "VERIFICATION-009 만료 배치가 실행 중입니다\n"
                + "\tat com.kafkick.batch.job.VerifyJobConfig.lambda$startRunStep$12"
                + "(VerifyJobConfig.java:781)\n"
                + "\tat org.springframework.batch.core.step.tasklet.TaskletStep...";

        BatchRunView view = BatchRunView.of(new BatchRun(1L, "verifyJob", "FAILED", "FAILED",
                realExitMessage, null, null, null, null));

        assertThat(view.failure())
                .as("도메인 코드가 있으면 그것만 싣는다")
                .isEqualTo("VERIFICATION-009");
        assertThat(view.failure())
                .doesNotContain("at com.kafkick")
                .doesNotContain("VerifyJobConfig.java");
    }

    @Test
    @DisplayName("도메인 코드가 없으면 예외 이름만 — 메시지 본문은 안 싣는다")
    void keepsOnlyTheExceptionTypeWhenThereIsNoDomainCode() {
        BatchRunView view = BatchRunView.of(new BatchRun(1L, "expireJob", "FAILED", "FAILED",
                "java.sql.SQLSyntaxErrorException: Unknown column 'foo' in 'where clause'",
                null, null, null, null));

        assertThat(view.failure()).isEqualTo("SQLSyntaxErrorException");
        assertThat(view.failure())
                .as("SQL 조각이 섞여 나가면 스키마가 밖으로 새는 셈이다")
                .doesNotContain("foo");
    }

    /**
     * 성공한 실행에도 EXIT_MESSAGE 가 채워진다 — SimpleJob 이 마지막 Step 의 ExitStatus 를
     * 통째로 대입하고 statsAggregateStep 이 성공 설명을 세운다. 아래 문자열은 실제 DB 에서
     * 그대로 가져온 것이다.
     */
    @Test
    @DisplayName("성공한 실행은 실패 요약이 비어 있다 — 안 거르면 초록 행마다 사유가 찍힌다")
    void hasNoFailureWhenSucceeded() {
        BatchRunView view = BatchRunView.of(new BatchRun(1L, "verifyJob", "COMPLETED",
                "COMPLETED", "회차 147 · 등급쌍 468 · 요일시각 168 · ISSUE 이력 3000000건",
                null, null, null, null));

        assertThat(view.failure())
                .as("이 문자열에는 도메인 코드도 예외 이름도 없어 요약기가 '알 수 없는 오류' 로 접는다")
                .isNull();
    }

    @Test
    @DisplayName("원인이 안 남은 실패는 그렇게 말한다 — null 로 접지 않는다")
    void saysWhenTheCauseWasNotRecorded() {
        BatchRunView view = BatchRunView.of(new BatchRun(1L, "expireJob", "FAILED", "FAILED",
                "", null, null, null, null));

        assertThat(view.failure()).isEqualTo(FailureSummary.NOT_RECORDED);
    }

    @Test
    @DisplayName("도메인 코드도 예외 이름도 없는 실패는 알 수 없다고 말한다")
    void saysWhenTheCauseIsUnrecognized() {
        BatchRunView view = BatchRunView.of(new BatchRun(1L, "expireJob", "FAILED", "FAILED",
                "무언가 잘못됐습니다", null, null, null, null));

        assertThat(view.failure()).isEqualTo(FailureSummary.UNKNOWN);
    }

    @Test
    @DisplayName("모르는 상태는 실패로 본다 — 규약 밖 문자열이 조용히 성공으로 접히면 안 된다")
    void treatsUnknownStatusAsFailure() {
        BatchRunView view = BatchRunView.of(new BatchRun(1L, "expireJob", "무엇인가", "?",
                "SQLSyntaxErrorException: ...", null, null, null, null));

        assertThat(view.failure()).isEqualTo("SQLSyntaxErrorException");
    }

    @Test
    @DisplayName("STOPPED 도 사유를 붙인다 — isUnsuccessful() 은 이걸 false 로 준다")
    void treatsStoppedRunAsFailure() {
        // 실측(6.0.4): STOPPED.isUnsuccessful() == false. 그것으로 갈랐다면 운영자가
        // 세운 실행이 완주한 실행과 응답에서 구분되지 않는다.
        assertThat(BatchStatus.STOPPED.isUnsuccessful())
                .as("이 전제가 뒤집히면 SUCCEEDED 열거를 다시 판단해야 한다")
                .isFalse();
        // 진짜 STOPPED 행이 무엇을 싣는지 실측했다(javap, 6.0.4):
        // getDefaultExitStatusForFailure 가 JobInterruptedException 이면
        // ExitStatus.STOPPED.addExitDescription(JobInterruptedException.class.getName()).
        assertThat(BatchRunView.of(runWith("STOPPED",
                "org.springframework.batch.core.job.JobInterruptedException")).failure())
                .isEqualTo("JobInterruptedException");
    }

    @Test
    @DisplayName("STATUS 가 NULL 이어도 목록이 산다 — Set.of.contains(null) 은 NPE 다")
    void survivesNullStatus() {
        assertThat(BatchRunView.of(runWith(null, "VERIFICATION-003 결정론 위반")).failure())
                .as("한 행 때문에 던지면 관제 히스토리가 통째로 500 이 된다")
                .isEqualTo("VERIFICATION-003");
    }

    @Test
    @DisplayName("도는 중에는 사유가 없다 — 있으면 화면이 '실패했는데 안 끝났다' 로 읽는다")
    void hasNoFailureWhileStillRunning() {
        assertThat(BatchRunView.of(runWith("STARTED", null)).failure()).isNull();
        assertThat(BatchRunView.of(runWith("STARTING", null)).failure()).isNull();
        assertThat(BatchRunView.of(runWith("STOPPING", null)).failure()).isNull();
    }

    private static BatchRun run(LocalDateTime startedAt, LocalDateTime finishedAt) {
        return new BatchRun(1L, "expireJob", "COMPLETED", "COMPLETED", "",
                startedAt, finishedAt, 0L, 340_529L);
    }

    private static BatchRun runWith(String status, String exitMessage) {
        return new BatchRun(1L, "verifyJob", status, status, exitMessage,
                LocalDateTime.of(2026, 1, 15, 9, 30, 7), null, 0L, 0L);
    }
}
