package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.verification.replay.IssuanceHistoryRecord;
import com.kafkick.core.verification.replay.ReplayScanRange;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

@RepositoryTest
@Import(ReplayHistoryJdbcAdapter.class)
class ReplayHistoryJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);
    private static final long NO_UPPER_BOUND = Long.MAX_VALUE;

    @Autowired
    private ReplayHistoryJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed data;

    @BeforeEach
    void setUp() {
        data = new VerificationSeed(jdbcClient);
    }

    // ─────────────────────────── 경계 산정 ───────────────────────────

    @Test
    @DisplayName("asOf 이하 이력을 가진 발급건의 양 끝을 준다")
    void reportIssuanceBounds() {
        long first = issuedIssuance(AS_OF.minusHours(1));
        long second = issuedIssuance(AS_OF.minusHours(1));

        ReplayScanRange range = adapter.scanRange(AS_OF).orElseThrow();

        assertThat(range.minIssuanceId()).isEqualTo(first);
        assertThat(range.maxIssuanceId()).isEqualTo(second);
    }

    @Test
    @DisplayName("asOf 이후에만 이력이 있는 발급건은 경계에서 빠진다")
    void excludeIssuanceWithOnlyFutureHistory() {
        long visible = issuedIssuance(AS_OF.minusHours(1));
        issuedIssuance(AS_OF.plusHours(1));

        ReplayScanRange range = adapter.scanRange(AS_OF).orElseThrow();

        assertThat(range.minIssuanceId()).isEqualTo(visible);
        assertThat(range.maxIssuanceId()).isEqualTo(visible);
    }

    @Test
    @DisplayName("asOf 이하 이력의 마지막 식별자를 준다 — 이게 실행 중 들어온 행을 막는 상한이다")
    void reportMaxHistoryIdWithinAsOf() {
        long issuanceId = data.issuance(IssuanceStatus.USED);
        long inside = data.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(1));
        data.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, AS_OF.plusHours(1));

        assertThat(adapter.scanRange(AS_OF).orElseThrow().maxHistoryId()).isEqualTo(inside);
    }

    @Test
    @DisplayName("전체 이력의 마지막 시각은 asOf 로 자르지 않는다 — asOf 가 과거인지 판정해야 한다")
    void reportLatestCreatedAtAcrossAllHistory() {
        LocalDateTime future = AS_OF.plusHours(1);
        issuedIssuance(AS_OF.minusHours(1));
        issuedIssuance(future);

        assertThat(adapter.scanRange(AS_OF).orElseThrow().latestCreatedAt()).isEqualTo(future);
    }

    @Test
    @DisplayName("asOf 이후 이력만 있으면 창은 없지만 마지막 시각은 준다 — 거부해야 하는 경우다")
    void reportRangeWithoutWindowWhenEveryHistoryIsAfterAsOf() {
        LocalDateTime future = AS_OF.plusHours(1);
        issuedIssuance(future);

        ReplayScanRange range = adapter.scanRange(AS_OF).orElseThrow();

        assertThat(range.hasWindow()).isFalse();
        assertThat(range.latestCreatedAt()).isEqualTo(future);
        assertThat(range.isBefore(AS_OF)).isTrue();
    }

    @Test
    @DisplayName("이력이 한 행도 없을 때만 빈 값이다")
    void reportEmptyOnlyWhenNoHistoryAtAll() {
        assertThat(adapter.scanRange(AS_OF)).isEmpty();
    }

    // ─────────────────────────── 구간 읽기 ───────────────────────────

    @Test
    @DisplayName("구간의 이력을 읽는다")
    void readHistoriesInRange() {
        long issuanceId = data.issuance(IssuanceStatus.USED);
        data.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(2));
        data.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, AS_OF.minusHours(1));

        List<IssuanceHistoryRecord> histories =
                adapter.findRange(issuanceId, issuanceId, AS_OF, NO_UPPER_BOUND);

        assertThat(histories).extracting(IssuanceHistoryRecord::eventType)
                .containsExactly(IssuanceEventType.ISSUE, IssuanceEventType.USE);
    }

    @Test
    @DisplayName("상한을 넘는 이력은 읽지 않는다 — 실행이 도는 동안 들어온 행이다")
    void excludeHistoryAboveUpperBound() {
        long issuanceId = data.issuance(IssuanceStatus.USED);
        long frozen = data.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(2));
        data.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, AS_OF.minusHours(1));

        assertThat(adapter.findRange(issuanceId, issuanceId, AS_OF, frozen))
                .singleElement()
                .extracting(IssuanceHistoryRecord::id)
                .isEqualTo(frozen);
    }

    @Test
    @DisplayName("상한과 같은 식별자는 읽는다 — 경계는 포함이다")
    void includeHistoryExactlyAtUpperBound() {
        long issuanceId = data.issuance(IssuanceStatus.ISSUED);
        long historyId = data.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(1));

        assertThat(adapter.findRange(issuanceId, issuanceId, AS_OF, historyId)).hasSize(1);
    }

    @Test
    @DisplayName("발급 이력의 from_status 는 null 로 읽힌다 — 발급 이전에는 상태가 없다")
    void readNullFromStatus() {
        long issuanceId = issuedIssuance(AS_OF.minusHours(1));

        assertThat(adapter.findRange(issuanceId, issuanceId, AS_OF, NO_UPPER_BOUND))
                .singleElement()
                .satisfies(history -> {
                    assertThat(history.fromStatus()).isNull();
                    assertThat(history.toStatus()).isEqualTo(IssuanceStatus.ISSUED);
                });
    }

    @Test
    @DisplayName("asOf 이후 이력은 읽지 않는다 — 미래를 보면 리플레이가 asOf 시점이 아니게 된다")
    void excludeHistoryAfterAsOf() {
        long issuanceId = data.issuance(IssuanceStatus.USED);
        data.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(1));
        data.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, AS_OF.plusHours(1));

        assertThat(adapter.findRange(issuanceId, issuanceId, AS_OF, NO_UPPER_BOUND)).hasSize(1);
    }

    @Test
    @DisplayName("asOf 와 같은 시각의 이력은 읽는다 — 경계는 포함이다")
    void includeHistoryExactlyAtAsOf() {
        long issuanceId = issuedIssuance(AS_OF);

        assertThat(adapter.findRange(issuanceId, issuanceId, AS_OF, NO_UPPER_BOUND)).hasSize(1);
    }

    @Test
    @DisplayName("구간 밖 발급건의 이력은 읽지 않는다 — 청크 경계가 새면 남의 이력을 접는다")
    void excludeHistoryOutsideRange() {
        long inside = issuedIssuance(AS_OF.minusHours(1));
        issuedIssuance(AS_OF.minusHours(1));

        assertThat(adapter.findRange(inside, inside, AS_OF, NO_UPPER_BOUND))
                .singleElement()
                .extracting(IssuanceHistoryRecord::issuanceId)
                .isEqualTo(inside);
    }

    @Test
    @DisplayName("구간 양 끝은 포함이다")
    void rangeBoundsAreInclusive() {
        long first = issuedIssuance(AS_OF.minusHours(1));
        long second = issuedIssuance(AS_OF.minusHours(1));

        assertThat(adapter.findRange(first, second, AS_OF, NO_UPPER_BOUND)).hasSize(2);
    }

    @Test
    @DisplayName("빈 구간은 빈 목록이다")
    void readEmptyRange() {
        assertThat(adapter.findRange(1L, 10L, AS_OF, NO_UPPER_BOUND)).isEmpty();
    }

    // ─────────────────────────── 정렬 ───────────────────────────

    @Test
    @DisplayName("발급건 순서로 묶여 나온다 — 리더가 연속 구간으로 경계를 가른다")
    void orderByIssuanceIdFirst() {
        long first = data.issuance(IssuanceStatus.USED);
        long second = data.issuance(IssuanceStatus.ISSUED);
        data.history(second, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(3));
        data.history(first, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(2));
        data.history(first, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, AS_OF.minusHours(1));

        assertThat(adapter.findRange(first, second, AS_OF, NO_UPPER_BOUND))
                .extracting(IssuanceHistoryRecord::issuanceId)
                .containsExactly(first, first, second);
    }

    @Test
    @DisplayName("같은 시각이면 이력 식별자로 가른다 — 타이브레이커가 없으면 접은 결과가 흔들린다")
    void breakTieByHistoryId() {
        long issuanceId = data.issuance(IssuanceStatus.USED);
        long issued = data.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF);
        long used = data.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, AS_OF);

        assertThat(adapter.findRange(issuanceId, issuanceId, AS_OF, NO_UPPER_BOUND))
                .extracting(IssuanceHistoryRecord::id)
                .containsExactly(issued, used);
    }

    // ─────────────────────────── 매핑 왕복 ───────────────────────────

    @Test
    @DisplayName("마이크로초가 왕복에서 살아남는다 — created_at 은 datetime(6) 이다")
    void preserveMicroseconds() {
        LocalDateTime precise = AS_OF.minusHours(1).withNano(123_456_000);
        long issuanceId = issuedIssuance(precise);

        assertThat(adapter.findRange(issuanceId, issuanceId, AS_OF, NO_UPPER_BOUND))
                .singleElement()
                .extracting(IssuanceHistoryRecord::createdAt)
                .isEqualTo(precise);
    }

    @Test
    @DisplayName("asOf 경계는 마이크로초까지 따진다 — 1마이크로초 뒤 이력은 빠진다")
    void cutAtAsOfWithMicrosecondPrecision() {
        LocalDateTime asOf = AS_OF.withNano(500_000_000);
        long issuanceId = data.issuance(IssuanceStatus.USED);
        data.history(issuanceId, IssuanceEventType.ISSUE, null, IssuanceStatus.ISSUED, asOf);
        data.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, asOf.plusNanos(1_000));

        assertThat(adapter.findRange(issuanceId, issuanceId, asOf, NO_UPPER_BOUND))
                .singleElement()
                .extracting(IssuanceHistoryRecord::eventType)
                .isEqualTo(IssuanceEventType.ISSUE);
    }

    @ParameterizedTest
    @EnumSource(IssuanceStatus.class)
    @DisplayName("네 가지 상태가 모두 그대로 왕복한다 — 하나라도 어긋나면 리플레이가 틀린 상태로 굳는다")
    void roundTripEveryStatus(IssuanceStatus status) {
        long issuanceId = data.issuance(status);
        data.history(issuanceId, IssuanceEventType.ISSUE, status, status, AS_OF.minusHours(1));

        assertThat(adapter.findRange(issuanceId, issuanceId, AS_OF, NO_UPPER_BOUND))
                .singleElement()
                .satisfies(history -> {
                    assertThat(history.fromStatus()).isEqualTo(status);
                    assertThat(history.toStatus()).isEqualTo(status);
                });
    }

    @ParameterizedTest
    @EnumSource(IssuanceEventType.class)
    @DisplayName("다섯 가지 사건이 모두 그대로 왕복한다")
    void roundTripEveryEventType(IssuanceEventType eventType) {
        long issuanceId = data.issuance(IssuanceStatus.ISSUED);
        data.history(issuanceId, eventType, null, IssuanceStatus.ISSUED, AS_OF.minusHours(1));

        assertThat(adapter.findRange(issuanceId, issuanceId, AS_OF, NO_UPPER_BOUND))
                .singleElement()
                .extracting(IssuanceHistoryRecord::eventType)
                .isEqualTo(eventType);
    }

    private long issuedIssuance(LocalDateTime createdAt) {
        long issuanceId = data.issuance(IssuanceStatus.ISSUED);
        data.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, createdAt);
        return issuanceId;
    }
}
