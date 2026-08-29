// 발급건 하나의 이력을 접어 asOf 시점 상태를 재구성합니다. Step 0 의 핵심입니다.
package com.kafkick.core.verification.replay;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import com.kafkick.core.coupon.domain.CouponStateMachine;
import com.kafkick.core.coupon.domain.IssuanceStatus;

/**
 * <b>상태는 {@code created_at <= asOf} 인 이력을 {@code (created_at, id)} 오름차순으로 정렬한
 * 마지막 행의 {@code to_status} 입니다.</b> asOf 필터는 호출자(SQL)가 겁니다 —
 * 여기 들어온 이력은 이미 asOf 이하라고 봅니다.
 *
 * <p>추적 상태는 <b>앞 행의 to_status</b>로만 갱신합니다. 불법 전이를 만나도 그 행의
 * {@code to_status} 를 따라갑니다. 멈추거나 되돌리면 뒤 행이 전부 연쇄로 불법이 되어
 * 위반 200건이 수천 건으로 번지고, 오염셋 집합 비교가 성립하지 않습니다.
 *
 * <p>{@code from_status} 는 대조에만 씁니다. 오염 주입이 이 값을 위조하면 이력만 읽어서는
 * 합법으로 보이기 때문입니다.
 */
public final class HistoryReplay {

    private static final Comparator<IssuanceHistoryRecord> REPLAY_ORDER =
            Comparator.comparing(IssuanceHistoryRecord::createdAt)
                    .thenComparingLong(IssuanceHistoryRecord::id);

    private HistoryReplay() {
    }

    /**
     * 발급건 하나의 이력을 접는다.
     *
     * @param issuanceId 접을 발급건
     * @param histories  그 발급건의 asOf 이하 이력. 순서는 보장하지 않아도 된다
     * @throws IllegalArgumentException 이력이 비었거나 다른 발급건의 이력이 섞였을 때
     */
    public static ReplayResult fold(long issuanceId, List<IssuanceHistoryRecord> histories) {
        if (histories == null || histories.isEmpty()) {
            throw new IllegalArgumentException("접을 이력이 없습니다. 발급건=" + issuanceId);
        }

        List<IssuanceHistoryRecord> ordered = histories.stream()
                .sorted(REPLAY_ORDER)
                .toList();

        List<IllegalTransition> illegalTransitions = new ArrayList<>();
        IssuanceStatus tracked = null;

        for (IssuanceHistoryRecord history : ordered) {
            if (history.issuanceId() != issuanceId) {
                throw new IllegalArgumentException(
                        "다른 발급건의 이력이 섞였습니다. 기대=" + issuanceId
                                + " 실제=" + history.issuanceId());
            }

            inspect(tracked, history).ifPresent(illegalTransitions::add);
            tracked = history.toStatus();
        }

        IssuanceHistoryRecord last = ordered.get(ordered.size() - 1);
        return new ReplayResult(
                issuanceId, tracked, last.id(), last.createdAt(), illegalTransitions);
    }

    /**
     * 이력 한 행이 낸 위반. <b>많아야 하나입니다</b> — uk_run_finding 이 같은 target_key 를 막습니다.
     *
     * <p>전이표를 먼저 봅니다. 추적 상태가 진실이므로 그것으로 판정한 결과가 더 확실합니다.
     * 전이가 합법일 때만 from_status 위조를 따로 확인합니다.
     *
     * <p><b>{@code isLegal} 로 묻는다 — 한 사건의 결과가 하나라고 보면 안 된다.</b>
     * {@code CANCEL_USE} 는 <b>결과가 둘</b>이다: 아직 안 만료면 {@code ISSUED} 로, 이미
     * 만료됐으면 {@code EXPIRED} 로 간다({@code CouponStateMachine.cancelUse}). 예전에는
     * 이 자리가 {@code (from, event) → to} 1:1 맵을 보고 <b>기대값 하나</b>와 비교했는데,
     * 그 표에는 {@code ISSUED} 만 있었다 — 만료된 쿠폰의 사용 취소가 실제로 일어나면
     * <b>정상 이력이 통째로 ILLEGAL_TRANSITION 오탐</b>이 되어 정상셋 0건 게이트가 깨진다.
     * 사용·취소 경로({@code CouponCancelUseService})가 그 갈래를 타므로 도달 가능하다.
     *
     * <p>대신 <b>기대값을 한 값으로 못 적는다.</b> 그래서 expected 자리에 <b>사건 이름</b>을
     * 넣어 <i>"이 사건으로는 그 상태에 갈 수 없다"</i> 를 말한다.
     */
    private static Optional<IllegalTransition> inspect(
            IssuanceStatus tracked,
            IssuanceHistoryRecord history
    ) {
        if (!CouponStateMachine.isLegal(tracked, history.eventType(), history.toStatus())) {
            return Optional.of(IllegalTransition.notInTable(
                    history.id(),
                    tracked,
                    history.eventType(),
                    history.toStatus()));
        }

        if (history.fromStatus() != tracked) {
            return Optional.of(IllegalTransition.chainBroken(
                    history.id(), tracked, history.fromStatus()));
        }

        return Optional.empty();
    }
}
