// 조회 응답의 시각이 도메인 축인지, 그리고 START_TIME 이 비어도 안 죽는지 확인합니다.
package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationRun;

/**
 * <b>이 조회는 축이 둘인 응답을 낸다.</b> {@code asOf} 는 도메인 값(UTC)이고
 * {@code startedAt}·{@code finishedAt} 은 배치 메타(JVM 기본 존)에서 온다 — 옮기지 않으면
 * 같은 실행이 {@code /verify/runs/{id}} 와 {@code /verify/report} 에서 <b>다른 답</b>을 낸다.
 *
 * <p><b>DB 없이 잰다.</b> {@code VerifyRunView.of} 는 순수 변환이라 컨텍스트가 필요 없고,
 * 여기서 재려는 것은 <b>그 변환</b>이지 잡의 동작이 아니다.
 */
class VerifyRunViewAxisTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /**
     * <b>이 갈래가 실제로 뚫렸다.</b> {@code AbstractJob.execute} 는 {@code START_TIME} 을
     * <b>태스크 실행기 스레드 안에서</b> 찍고 상태가 {@code STOPPING} 이면 아예 안 찍는다
     * (6.0.4 바이트코드). 트리거가 비동기라 {@code 202} 직후 폴링이 그 창에 들어가고,
     * 실행기가 거절해 {@code FAILED} 로 남은 행은 <b>영원히</b> 비어 있다.
     *
     * <p>축 변환을 넣으면서 널가드가 사라져 <b>인증 없는 조회가 500 을 냈다</b> — 이 API 를
     * 연 이유가 <i>"폴링해야 원인을 안다"</i> 를 없애는 것인데 그 첫 호출이 깨졌다.
     */
    @Test
    @DisplayName("START_TIME 이 비어도 안 죽는다 — 202 직후 폴링이 그 창에 들어간다")
    void survivesMissingStartTime() {
        JobExecution execution = execution(null, null, BatchStatus.STARTING);

        assertThatCode(() -> VerifyRunView.of(1L, execution, null, Optional.empty()))
                .doesNotThrowAnyException();
        assertThat(VerifyRunView.of(1L, execution, null, Optional.empty()).startedAt())
                .as("없는 값은 없는 채로 나가야 한다 — 던지면 규약에 없는 500 이 된다")
                .isNull();
    }

    @Test
    @DisplayName("실행 행이 없으면 배치 메타 시각을 도메인 축으로 옮겨 내보낸다")
    void movesBatchMetaTimesWhenRunRowIsMissing() {
        assumeThat(ZoneId.systemDefault().getRules().getOffset(Instant.now()))
                .as("UTC JVM 에서는 변환이 항등이라 이 축을 못 잰다")
                .isNotEqualTo(ZoneOffset.UTC);

        LocalDateTime jvmWall = LocalDateTime.of(2026, 1, 15, 18, 30, 7);
        LocalDateTime utcWall = jvmWall.atZone(ZoneId.systemDefault())
                .withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();

        VerifyRunView view = VerifyRunView.of(1L,
                execution(jvmWall, jvmWall.plusMinutes(8), BatchStatus.COMPLETED),
                null, Optional.empty());

        assertThat(view.startedAt())
                .as("배치 메타 축을 그대로 내보내면 같은 응답 안의 asOf(UTC)와 좌표계가 갈린다")
                .isEqualTo(utcWall)
                .isNotEqualTo(jvmWall);
        assertThat(view.finishedAt())
                .as("종료도 같은 처리를 받아야 한다 — 한쪽만 옮기면 간격이 오프셋만큼 벌어진다")
                .isEqualTo(utcWall.plusMinutes(8));
    }

    /**
     * <b>실행 행이 있으면 그 값을 그대로 쓴다.</b> 그것은 {@code startRunStep} 이 이미 도메인
     * 축으로 넣은 값이라, 여기서 <b>또 옮기면 두 번 밀린다</b>.
     */
    @Test
    @DisplayName("실행 행이 있으면 그 값을 쓴다 — 두 번 옮기지 않는다")
    void prefersTheDomainRowWithoutMovingItAgain() {
        LocalDateTime domainStartedAt = LocalDateTime.of(2026, 1, 15, 9, 30, 7);
        VerificationRun run = VerificationRun.start(AS_OF, null, ScopeType.FULL,
                DatasetType.CLEAN, 1, domainStartedAt);

        VerifyRunView view = VerifyRunView.of(1L,
                execution(LocalDateTime.of(2026, 1, 15, 18, 30, 7), null, BatchStatus.STARTED),
                null, Optional.of(run));

        assertThat(view.startedAt())
                .as("도메인 값을 또 변환하면 오프셋만큼 두 번 밀린다")
                .isEqualTo(domainStartedAt);
    }

    private static JobExecution execution(LocalDateTime startTime, LocalDateTime endTime,
            BatchStatus status) {
        JobExecution execution = new JobExecution(
                1L, new JobInstance(1L, "verifyJob"), new JobParameters());
        execution.setStatus(status);
        execution.setStartTime(startTime);
        execution.setEndTime(endTime);
        return execution;
    }
}
