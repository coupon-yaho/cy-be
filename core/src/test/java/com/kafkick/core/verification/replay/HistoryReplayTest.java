package com.kafkick.core.verification.replay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;

class HistoryReplayTest {

    private static final long ISSUANCE_ID = 7L;
    private static final LocalDateTime T0 = LocalDateTime.of(2026, 8, 15, 10, 0);

    @Test
    @DisplayName("발급만 있으면 ISSUED 다")
    void foldIssueOnly() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                issue(1L, T0)));

        assertThat(result.state()).isEqualTo(IssuanceStatus.ISSUED);
        assertThat(result.hasIllegalTransition()).isFalse();
    }

    @Test
    @DisplayName("발급 다음 사용이면 USED 다")
    void foldIssueThenUse() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                issue(1L, T0),
                history(2L, T0.plusMinutes(1), IssuanceEventType.USE,
                        IssuanceStatus.ISSUED, IssuanceStatus.USED)));

        assertThat(result.state()).isEqualTo(IssuanceStatus.USED);
        assertThat(result.illegalTransitions()).isEmpty();
    }

    @Test
    @DisplayName("사용취소는 USED 를 ISSUED 로 되돌린다 — 역방향이라 재고가 양방향으로 움직인다")
    void foldCancelUseGoesBack() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                issue(1L, T0),
                history(2L, T0.plusMinutes(1), IssuanceEventType.USE,
                        IssuanceStatus.ISSUED, IssuanceStatus.USED),
                history(3L, T0.plusMinutes(2), IssuanceEventType.CANCEL_USE,
                        IssuanceStatus.USED, IssuanceStatus.ISSUED)));

        assertThat(result.state()).isEqualTo(IssuanceStatus.ISSUED);
        assertThat(result.illegalTransitions()).isEmpty();
    }

    @Test
    @DisplayName("이미 쓴 쿠폰은 만료되지 않는다 — USED 에서 EXPIRE 는 전이표에 없다")
    void rejectExpireOnUsed() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                issue(1L, T0),
                history(2L, T0.plusMinutes(1), IssuanceEventType.USE,
                        IssuanceStatus.ISSUED, IssuanceStatus.USED),
                history(3L, T0.plusMinutes(2), IssuanceEventType.EXPIRE,
                        IssuanceStatus.USED, IssuanceStatus.EXPIRED)));

        assertThat(result.illegalTransitions()).singleElement().satisfies(illegal -> {
            assertThat(illegal.historyId()).isEqualTo(3L);
            assertThat(illegal.reason()).isEqualTo(IllegalTransition.Reason.NOT_IN_TABLE);
            assertThat(illegal.expected()).isEqualTo("USED-EXPIRE->?");
            assertThat(illegal.actual()).isEqualTo("USED-EXPIRE->EXPIRED");
        });
    }

    @Test
    @DisplayName("종단 상태에서 되살리면 잡는다 — 오염 유형 4 가 겨냥하는 것")
    void rejectReviveFromTerminal() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                issue(1L, T0),
                history(2L, T0.plusMinutes(1), IssuanceEventType.CANCEL,
                        IssuanceStatus.ISSUED, IssuanceStatus.CANCELLED),
                history(3L, T0.plusMinutes(2), IssuanceEventType.USE,
                        IssuanceStatus.CANCELLED, IssuanceStatus.USED)));

        assertThat(result.illegalTransitions()).singleElement()
                .extracting(IllegalTransition::historyId)
                .isEqualTo(3L);
    }

    @Test
    @DisplayName("전이는 합법인데 from_status 가 거짓이면 잡는다 — 이력만 읽으면 안 보인다")
    void detectForgedFromStatus() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                issue(1L, T0),
                history(2L, T0.plusMinutes(1), IssuanceEventType.USE,
                        IssuanceStatus.EXPIRED, IssuanceStatus.USED)));

        assertThat(result.state()).isEqualTo(IssuanceStatus.USED);
        assertThat(result.illegalTransitions()).singleElement().satisfies(illegal -> {
            assertThat(illegal.reason()).isEqualTo(IllegalTransition.Reason.CHAIN_BROKEN);
            assertThat(illegal.expected()).isEqualTo("from=ISSUED");
            assertThat(illegal.actual()).isEqualTo("from=EXPIRED");
        });
    }

    @Test
    @DisplayName("한 이력 행은 위반을 하나만 낸다 — uk_run_finding 이 같은 target_key 를 막는다")
    void emitAtMostOneFindingPerHistory() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                issue(1L, T0),
                history(2L, T0.plusMinutes(1), IssuanceEventType.CANCEL_USE,
                        IssuanceStatus.USED, IssuanceStatus.ISSUED)));

        assertThat(result.illegalTransitions()).hasSize(1);
        assertThat(result.illegalTransitions()).extracting(IllegalTransition::historyId)
                .doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("첫 이력이 발급이 아니면 잡는다")
    void rejectFirstHistoryOtherThanIssue() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                history(1L, T0, IssuanceEventType.USE, null, IssuanceStatus.USED)));

        assertThat(result.state()).isEqualTo(IssuanceStatus.USED);
        assertThat(result.illegalTransitions()).singleElement().satisfies(illegal -> {
            assertThat(illegal.reason()).isEqualTo(IllegalTransition.Reason.NOT_IN_TABLE);
            assertThat(illegal.expected()).isEqualTo("(없음)-USE->?");
        });
    }

    @Test
    @DisplayName("불법 전이를 만나도 멈추지 않고 계속 접는다 — 멈추면 뒤가 전부 연쇄로 번진다")
    void keepFoldingAfterIllegalTransition() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                issue(1L, T0),
                history(2L, T0.plusMinutes(1), IssuanceEventType.EXPIRE,
                        IssuanceStatus.ISSUED, IssuanceStatus.USED),
                history(3L, T0.plusMinutes(2), IssuanceEventType.CANCEL_USE,
                        IssuanceStatus.USED, IssuanceStatus.ISSUED)));

        assertThat(result.state()).isEqualTo(IssuanceStatus.ISSUED);
        assertThat(result.illegalTransitions()).extracting(IllegalTransition::historyId)
                .containsExactly(2L);
    }

    @Test
    @DisplayName("순서가 뒤섞여 들어와도 created_at 오름차순으로 접는다")
    void foldInCreatedAtOrder() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                history(2L, T0.plusMinutes(1), IssuanceEventType.USE,
                        IssuanceStatus.ISSUED, IssuanceStatus.USED),
                issue(1L, T0)));

        assertThat(result.state()).isEqualTo(IssuanceStatus.USED);
        assertThat(result.illegalTransitions()).isEmpty();
    }

    @Test
    @DisplayName("created_at 이 같으면 id 로 가른다 — 동시 발생 이력의 순서를 결정론으로 만든다")
    void breakTieById() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                history(2L, T0, IssuanceEventType.USE,
                        IssuanceStatus.ISSUED, IssuanceStatus.USED),
                issue(1L, T0)));

        assertThat(result.state()).isEqualTo(IssuanceStatus.USED);
        assertThat(result.lastHistoryId()).isEqualTo(2L);
        assertThat(result.illegalTransitions()).isEmpty();
    }

    @Test
    @DisplayName("마지막 이력의 id 와 시각을 남긴다 — asof_state 가 이 둘을 싣는다")
    void keepLastHistoryPointer() {
        LocalDateTime lastAt = T0.plusMinutes(2);
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(
                issue(1L, T0),
                history(9L, lastAt, IssuanceEventType.USE,
                        IssuanceStatus.ISSUED, IssuanceStatus.USED)));

        assertThat(result.lastHistoryId()).isEqualTo(9L);
        assertThat(result.lastEventAt()).isEqualTo(lastAt);
    }

    @Test
    @DisplayName("빈 이력은 거부한다 — 이력이 없으면 asOf 시점에 존재하지 않는 발급건이다")
    void rejectEmptyHistories() {
        assertThatThrownBy(() -> HistoryReplay.fold(ISSUANCE_ID, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("접을 이력이 없습니다");
    }

    @Test
    @DisplayName("다른 발급건의 이력이 섞이면 거부한다 — 구간 청크가 경계를 잘못 자른 것이다")
    void rejectForeignIssuanceHistory() {
        assertThatThrownBy(() -> HistoryReplay.fold(ISSUANCE_ID, List.of(
                issue(1L, T0),
                new IssuanceHistoryRecord(2L, 99L, IssuanceEventType.USE,
                        IssuanceStatus.ISSUED, IssuanceStatus.USED, T0.plusMinutes(1)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("다른 발급건의 이력이 섞였습니다");
    }

    @Test
    @DisplayName("위반 목록은 바꿀 수 없다")
    void illegalTransitionsAreImmutable() {
        ReplayResult result = HistoryReplay.fold(ISSUANCE_ID, List.of(issue(1L, T0)));

        assertThatThrownBy(() -> result.illegalTransitions().add(
                IllegalTransition.chainBroken(1L, null, IssuanceStatus.ISSUED)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static IssuanceHistoryRecord issue(long id, LocalDateTime createdAt) {
        return history(id, createdAt, IssuanceEventType.ISSUE, null, IssuanceStatus.ISSUED);
    }

    private static IssuanceHistoryRecord history(
            long id,
            LocalDateTime createdAt,
            IssuanceEventType eventType,
            IssuanceStatus fromStatus,
            IssuanceStatus toStatus
    ) {
        return new IssuanceHistoryRecord(
                id, ISSUANCE_ID, eventType, fromStatus, toStatus, createdAt);
    }
}
