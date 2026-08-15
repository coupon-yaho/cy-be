// 발급건 상태 전이표를 전수 검증합니다. 리플레이 전체가 이 판정에 의존합니다.
package com.kafkick.core.coupon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

class CouponStateMachineTest {

    @ParameterizedTest(name = "{0} + {1} → {2}")
    @CsvSource({
            ",           ISSUE,      ISSUED",
            "ISSUED,     USE,        USED",
            "USED,       CANCEL_USE, ISSUED",
            "ISSUED,     CANCEL,     CANCELLED",
            "ISSUED,     EXPIRE,     EXPIRED"
    })
    @DisplayName("합법 전이 다섯 가지는 결과 상태를 돌려준다")
    void allowLegalTransitions(
            IssuanceStatus from,
            IssuanceEventType event,
            IssuanceStatus expected
    ) {
        Optional<IssuanceStatus> next = CouponStateMachine.next(from, event);

        assertThat(next).contains(expected);
        assertThat(CouponStateMachine.isLegal(from, event, expected)).isTrue();
    }

    @Test
    @DisplayName("USED 에서 EXPIRE 는 불가하다 — 이미 쓴 쿠폰은 만료되지 않는다")
    void rejectExpireFromUsed() {
        assertThat(CouponStateMachine.next(IssuanceStatus.USED, IssuanceEventType.EXPIRE))
                .isEmpty();
    }

    @ParameterizedTest
    @EnumSource(IssuanceEventType.class)
    @DisplayName("종단 상태에서는 어떤 사건도 합법이 아니다")
    void rejectAnyEventFromTerminalStatus(IssuanceEventType event) {
        assertThat(CouponStateMachine.next(IssuanceStatus.CANCELLED, event)).isEmpty();
        assertThat(CouponStateMachine.next(IssuanceStatus.EXPIRED, event)).isEmpty();
    }

    @Test
    @DisplayName("오염 유형 4 — 종단 상태 EXPIRED 에서 USED 로 되돌리는 이력을 거부한다")
    void rejectRevivalFromExpired() {
        boolean legal = CouponStateMachine.isLegal(
                IssuanceStatus.EXPIRED,
                IssuanceEventType.USE,
                IssuanceStatus.USED
        );

        assertThat(legal).isFalse();
    }

    @Test
    @DisplayName("오염 유형 3 — CANCEL_USE 를 두 번 심으면 두 번째는 ISSUED 에서 오므로 거부된다")
    void rejectSecondCancelUse() {
        Optional<IssuanceStatus> afterFirst =
                CouponStateMachine.next(IssuanceStatus.USED, IssuanceEventType.CANCEL_USE);
        assertThat(afterFirst).contains(IssuanceStatus.ISSUED);

        Optional<IssuanceStatus> afterSecond =
                CouponStateMachine.next(afterFirst.orElseThrow(), IssuanceEventType.CANCEL_USE);

        assertThat(afterSecond).isEmpty();
    }

    @Test
    @DisplayName("발급 이전 상태에서 ISSUE 가 아닌 사건은 거부된다")
    void rejectNonIssueBeforeIssuance() {
        assertThat(CouponStateMachine.next(null, IssuanceEventType.USE)).isEmpty();
        assertThat(CouponStateMachine.next(null, IssuanceEventType.CANCEL)).isEmpty();
        assertThat(CouponStateMachine.next(null, IssuanceEventType.EXPIRE)).isEmpty();
        assertThat(CouponStateMachine.next(null, IssuanceEventType.CANCEL_USE)).isEmpty();
    }

    @Test
    @DisplayName("이미 발급된 건에 ISSUE 가 다시 오면 거부된다")
    void rejectDuplicateIssue() {
        assertThat(CouponStateMachine.next(IssuanceStatus.ISSUED, IssuanceEventType.ISSUE))
                .isEmpty();
    }

    @Test
    @DisplayName("전이표에 있어도 to_status 가 다르면 불법으로 본다")
    void rejectWrongToStatus() {
        boolean legal = CouponStateMachine.isLegal(
                IssuanceStatus.ISSUED,
                IssuanceEventType.USE,
                IssuanceStatus.CANCELLED
        );

        assertThat(legal).isFalse();
    }

    @Test
    @DisplayName("사건 종류가 없으면 거부한다")
    void rejectNullEvent() {
        assertThatThrownBy(() -> CouponStateMachine.next(IssuanceStatus.ISSUED, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("사건 종류가 필요합니다.");
    }

    @Test
    @DisplayName("종단 상태는 CANCELLED 와 EXPIRED 둘뿐이다")
    void identifyTerminalStatuses() {
        assertThat(CouponStateMachine.isTerminal(IssuanceStatus.CANCELLED)).isTrue();
        assertThat(CouponStateMachine.isTerminal(IssuanceStatus.EXPIRED)).isTrue();
        assertThat(CouponStateMachine.isTerminal(IssuanceStatus.ISSUED)).isFalse();
        assertThat(CouponStateMachine.isTerminal(IssuanceStatus.USED)).isFalse();
        assertThat(CouponStateMachine.isTerminal(null)).isFalse();
    }

    @Test
    @DisplayName("재고 불변식의 분자는 ISSUED 와 USED 뿐이다")
    void countOnlyIssuedAndUsedTowardStock() {
        assertThat(IssuanceStatus.ISSUED.countsTowardStock()).isTrue();
        assertThat(IssuanceStatus.USED.countsTowardStock()).isTrue();
        assertThat(IssuanceStatus.CANCELLED.countsTowardStock()).isFalse();
        assertThat(IssuanceStatus.EXPIRED.countsTowardStock()).isFalse();
    }

    @Test
    @DisplayName("전이표는 정확히 다섯 가지다 — 늘어나면 이 테스트가 먼저 깨진다")
    void keepExactlyFiveLegalTransitions() {
        long legalCount = 0;

        for (IssuanceEventType event : IssuanceEventType.values()) {
            if (CouponStateMachine.next(null, event).isPresent()) {
                legalCount++;
            }
            for (IssuanceStatus from : IssuanceStatus.values()) {
                if (CouponStateMachine.next(from, event).isPresent()) {
                    legalCount++;
                }
            }
        }

        assertThat(legalCount).isEqualTo(5);
    }
}
