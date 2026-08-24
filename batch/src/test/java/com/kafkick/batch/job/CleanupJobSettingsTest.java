// 정리 잡 설정값이 기동 시점에 걸러지는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>가드는 그 자체가 안전장치라 안 재면 지워져도 아무도 모른다.</b> 여섯 다 어겼을 때의
 * 결말이 <i>"조용히 이상해진다"</i> 라서, 발화 시점이 아니라 기동 시점에 막는다.
 *
 * <p><b>{@code chunk-size = 0} 이 특히 조용하다.</b> MySQL 은 {@code LIMIT 0} 을 오류로 보지
 * 않고 0건을 돌려주므로 첫 청크가 곧 종료 신호가 되고, 잡은 {@code COMPLETED} 로 닫힌다 —
 * {@code CleanupNotSucceeding} 도 안 울어서 <b>가장 무거운 테이블만 안 걷히는 상태가
 * 감시망을 통째로 통과한다.</b> {@code ExpireJobSettingsTest} 가 만료 쪽에서 같은 축을 잰다.
 *
 * <p><b>축은 여섯이다</b> — {@code step-timeout-ms}(초 단위 내림), {@code chunk-size}(0 이면
 * {@code asof_state} 를 한 행도 안 걷는다), {@code abandoned-after-hours}(0 이면 방금 시작한
 * 검증의 입력을 걷는다), {@code asof-state-keep-runs}(0 이면 직전 판정의 근거가 사라진다),
 * {@code metadata-keep-days}(되읽기 창 안쪽으로 내리면 연속 실패한 날 마지막 성공이 지워진다),
 * {@code metadata-chunk-size}(0 이면 {@code LIMIT 0} 이 조용히 0건, 너무 크면 한 트랜잭션이
 * 데드라인에 걸려 전량 롤백된다).
 *
 * <p><b>컨텍스트를 안 띄운다.</b> 이 검사들은 생성자 안에 있고 DB 와 아무 상관이 없다.
 * 그래서 값 검사를 Step 빈 메서드 파라미터가 아니라 생성자로 올려 뒀다 — 그러지 않으면
 * 잘못된 값 하나를 재는 데 Testcontainers MySQL 이 하나씩 뜬다.
 */
class CleanupJobSettingsTest {

    private static final long VALID_TIMEOUT = 120_000L;
    private static final int VALID_KEEP = 5;
    private static final int VALID_CHUNK = 10_000;
    private static final long VALID_ABANDONED_HOURS = 24L;

    private static final int VALID_KEEP_DAYS = 30;

    private static final int VALID_META_CHUNK = 500;

    @Test
    @DisplayName("기본값 조합은 기동한다 — 가드가 정상까지 막으면 안 된다")
    void acceptsDefaults() {
        assertThatCode(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK, VALID_ABANDONED_HOURS))
                .doesNotThrowAnyException();
    }

    /**
     * 스프링의 트랜잭션 타임아웃은 초 단위라 999 는 <b>0 으로 내려앉는다.</b> 0 은 "무제한" 이
     * 아니라 <b>데드라인이 이미 지났음</b>이라, 첫 문장에서 {@code TransactionTimedOutException}
     * 이 난다 — 기동은 성공하고 04:30 만 매일 조용히 실패하는 모양이 된다.
     */
    @Test
    @DisplayName("step-timeout-ms 가 1000 미만이면 기동하지 못한다")
    void rejectSubSecondTimeout() {
        assertThatThrownBy(() -> config(999, VALID_KEEP, VALID_CHUNK, VALID_ABANDONED_HOURS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.step-timeout-ms");
    }

    /** 0 은 1000 의 배수라 배수 검사로는 안 걸린다. 하한이 유일한 방어다. */
    @Test
    @DisplayName("step-timeout-ms 가 0 이어도 기동하지 못한다")
    void rejectZeroTimeout() {
        assertThatThrownBy(() -> config(0, VALID_KEEP, VALID_CHUNK, VALID_ABANDONED_HOURS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.step-timeout-ms");
    }

    /** 1500 은 1 초로 잘려 <b>적어 준 값보다 짧게</b> 끊기는데, 그 사실이 어디에도 안 남는다. */
    @Test
    @DisplayName("step-timeout-ms 가 1000 의 배수가 아니면 기동하지 못한다")
    void rejectNonMultipleTimeout() {
        assertThatThrownBy(() -> config(1_500, VALID_KEEP, VALID_CHUNK, VALID_ABANDONED_HOURS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.step-timeout-ms");
    }

    @Test
    @DisplayName("chunk-size 가 0 이면 기동하지 못한다")
    void rejectZeroChunkSize() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, VALID_KEEP, 0, VALID_ABANDONED_HOURS))
                .as("DELETE … LIMIT 0 은 오류 없이 0건을 돌려줘 잡이 성공으로 닫힌다")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.chunk-size");
    }

    @Test
    @DisplayName("chunk-size 가 음수여도 기동하지 못한다")
    void rejectNegativeChunkSize() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, VALID_KEEP, -1, VALID_ABANDONED_HOURS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.chunk-size");
    }

    /** 0 이면 방금 시작한 검증까지 "버려진 것" 으로 보고 <b>그 판정의 입력</b>을 걷는다. */
    @Test
    @DisplayName("abandoned-after-hours 가 0 이면 기동하지 못한다")
    void rejectZeroAbandonedWindow() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.abandoned-after-hours");
    }

    /** 0 이면 방금 끝난 판정의 파생 행까지 그날 밤에 사라져 되짚을 근거가 없다. */
    @Test
    @DisplayName("asof-state-keep-runs 가 0 이면 기동하지 못한다")
    void rejectZeroKeepRuns() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, 0, VALID_CHUNK, VALID_ABANDONED_HOURS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.verify.asof-state-keep-runs");
    }

    /**
     * <b>보존 기간이 되읽기 창보다 짧으면 정리 잡이 감시를 끈다.</b>
     * {@code BatchRunMetricsRefresher}·{@code ExpirePendingRefresher} 가 마지막 성공 실행을
     * {@code END_TIME > NOW() - INTERVAL 7 DAY} 창에서 찾으므로, 그 안쪽을 지우면 게이지가
     * {@code NaN} 이 되고 {@code ExpireNeverSucceeded}·{@code CleanupNeverSucceeded} 가
     * <b>영구 발화</b>한다. 그 관계는 SQL 리터럴과 설정 키 두 곳에 나뉘어 있어 코드가
     * 못 잇는다 — 기동에서 막는 것이 유일한 방어다.
     */
    @Test
    @DisplayName("metadata-keep-days 가 되읽기 창(7일) 이하면 기동하지 못한다")
    void rejectKeepDaysInsideRefreshWindow() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK,
                VALID_ABANDONED_HOURS, 7))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.metadata-keep-days");

        assertThatCode(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK,
                VALID_ABANDONED_HOURS, 8))
                .as("창 밖이면 통과해야 한다 — 하한이 상한처럼 굳으면 안 된다")
                .doesNotThrowAnyException();
    }

    private static CleanupJobConfig config(long timeoutMillis, int keepRuns, int chunkSize,
            long abandonedAfterHours) {
        return config(timeoutMillis, keepRuns, chunkSize, abandonedAfterHours, VALID_KEEP_DAYS);
    }

    /**
     * <b>{@code batch.cleanup.metadata-chunk-size = 0} 이 특히 조용하다.</b> MySQL 은 {@code LIMIT 0} 을
     * 오류로 안 보고 0건을 돌려주므로 첫 청크가 곧 종료 신호가 되고, 잡은 {@code COMPLETED}
     * 로 닫힌다 — {@code CleanupNotSucceeding} 도 안 울어서 <b>배치 메타만 영원히 안 걷히는
     * 상태가 감시망을 통째로 통과한다.</b> 이 클래스가 형제 키에 대해 이미 세운 판단이다.
     */
    @Test
    @DisplayName("metadata-chunk-size 가 0 이면 기동하지 못한다")
    void rejectZeroMetadataChunkSize() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK,
                VALID_ABANDONED_HOURS, VALID_KEEP_DAYS, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.metadata-chunk-size");
    }

    /**
     * <b>형제 키와 커버리지를 맞춘다.</b> {@code batch.cleanup.chunk-size} 에는 음수 케이스가
     * 있는데 이쪽에만 없으면, 하한을 {@code == 0} 으로 좁히는 리팩터가 조용히 통과한다.
     */
    @Test
    @DisplayName("metadata-chunk-size 가 음수여도 기동하지 못한다")
    void rejectNegativeMetadataChunkSize() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK,
                VALID_ABANDONED_HOURS, VALID_KEEP_DAYS, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.metadata-chunk-size");
    }

    /**
     * <b>상한도 가드다.</b> 삭제가 id 하나씩 나가므로 이 값은 <b>한 트랜잭션의 문장 수 ÷ 6</b>
     * 이다. 잠그는 행은 이 값이 아니라 그 실행들에 딸린 행 전부다 — {@code verifyJob} 이면
     * 실행 하나에 26행이라 5000 이면 13만 행이 한 트랜잭션에 들어간다. 형제 키
     * {@code batch.cleanup.chunk-size} 는 {@code LIMIT} 으로만 쓰여 이 성질이 없다.
     */
    @Test
    @DisplayName("metadata-chunk-size 가 5000 을 넘으면 기동하지 못한다")
    void rejectOversizedMetadataChunkSize() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK,
                VALID_ABANDONED_HOURS, VALID_KEEP_DAYS, 5_001))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.metadata-chunk-size");
        assertThatCode(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK,
                VALID_ABANDONED_HOURS, VALID_KEEP_DAYS, 5_000))
                .as("경계값 자체는 통과해야 한다 — 안 그러면 상한이 4999 인 셈이다")
                .doesNotThrowAnyException();
    }

    /**
     * <b>보존 기간에도 상한이 있다.</b> 없으면 오타 하나({@code 300})가
     * {@code chunk-size = 0} 과 <b>관측상 같은 상태</b>를 만든다 — 배치 메타가 사실상 안
     * 걷히는데 잡은 매일 {@code COMPLETED} 라 아무 알림도 안 운다.
     */
    @Test
    @DisplayName("metadata-keep-days 가 365 를 넘으면 기동하지 못한다")
    void rejectOversizedMetadataKeepDays() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK,
                VALID_ABANDONED_HOURS, 366))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.metadata-keep-days");
        assertThatCode(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK,
                VALID_ABANDONED_HOURS, 365))
                .as("경계값 자체는 통과해야 한다 — 안 그러면 상한이 364 인 셈이다")
                .doesNotThrowAnyException();
    }

    private static CleanupJobConfig config(long timeoutMillis, int keepRuns, int chunkSize,
            long abandonedAfterHours, int metadataKeepDays) {
        return config(timeoutMillis, keepRuns, chunkSize, abandonedAfterHours, metadataKeepDays,
                VALID_META_CHUNK);
    }

    private static CleanupJobConfig config(long timeoutMillis, int keepRuns, int chunkSize,
            long abandonedAfterHours, int metadataKeepDays, int metadataChunkSize) {
        return new CleanupJobConfig(null, null, timeoutMillis, keepRuns, chunkSize,
                abandonedAfterHours, metadataKeepDays, metadataChunkSize);
    }
}
