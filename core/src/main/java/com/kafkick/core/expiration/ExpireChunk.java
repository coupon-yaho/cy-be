// 한 청크가 손댈 범위입니다. 회차 하나와 그 안의 id 상한으로 이루어집니다.
package com.kafkick.core.expiration;

import java.util.List;

/**
 * <b>한 청크는 회차 하나만 건드린다.</b> 후보를 id 순으로 받아 <b>첫 회차의 연속부</b>까지만
 * 자른 결과다.
 *
 * <h2>왜 회차를 하나로 묶나</h2>
 *
 * <p>쓰는 세 문장이 전부 {@code coupon_id = :couponId} 로 닫혀 있기 때문이다 —
 * {@code expireBatch} · {@code appendExpireHistories} · {@code releaseStock}. 청크가 여러
 * 회차에 걸치면 그 셋을 회차 수만큼 반복해야 하고, {@code releaseStock} 이 첫 회차 것만
 * 빼면 <b>재고가 조용히 샌다.</b>
 *
 * <p><b>락 보유 구간과는 상관이 없다.</b> 한때 이 자리에 <i>"만료가 재고 행을 먼저 잠그니
 * 여러 회차면 그 전부를 쥔다"</i> 가 적혀 있었는데, 재고를 마지막에 잡게 된 뒤로는
 * ({@code docs/12} §11.1) 그 근거가 통째로 사라진다. 지금 만료가 재고 행을 쥐는 구간은
 * {@code lockStock} 부터 커밋까지 <b>한 문장</b>이고, 청크에 회차가 몇 개든 그 사이
 * 발급은 안 선다.
 *
 * <p><b>그래도 회차를 하나로 묶는다.</b> 이유가 락이 아니라 <b>SQL 의 모양</b>이기 때문이다 —
 * 위 세 문장이 회차 하나만 받는다. 근거가 바뀌었다고 이 자르기를 지우면
 * {@code releaseStock} 이 첫 회차 것만 빼고 나머지가 조용히 샌다.
 *
 * <h2>짧아지는 정도</h2>
 *
 * <p>시드에서는 발급건 id 가 <b>회차별로 완전히 뭉쳐 있다</b> — 생성기가
 * {@code for coupon in catalog.coupons} 로 회차 단위로 돌면서 id 를 증가시킨다
 * ({@code cy-seed} 의 {@code seedgen/issuances.py}). 회차 147 · 회차당 약 2만 건이라
 * 짧아지는 청크는 <b>경계 147개</b>뿐이다.
 *
 * <p><b>운영은 다를 수 있다.</b> 회차가 동시에 열려 있으면 발급 id 가 회차끼리 엇갈려서
 * 연속부가 짧아지고, 극단적으로는 청크마다 한 건씩만 처리한다. 그 조짐은
 * {@code cy_expire_chunk_fill} 로 나간다 — <b>충전율이 낮게 이어지면</b> 회차 단위
 * 커서로 바꿀 때다. 지금 그것을 미리 만들지 않는 것은, 시드에서 그 상태를 재현할 수 없어서
 * <b>고쳤는지 확인할 방법이 없기 때문</b>이다.
 */
public record ExpireChunk(long couponId, long lastId, int size) {

    /** 후보가 없는 상태. 잡의 <b>종료 신호</b>다. */
    public static final ExpireChunk EMPTY = new ExpireChunk(0L, 0L, 0);

    public ExpireChunk {
        if (size < 0) {
            throw new IllegalArgumentException("청크 크기는 음수일 수 없습니다. size=" + size);
        }
    }

    /**
     * id 오름차순 후보에서 <b>첫 회차의 연속부</b>를 잘라 낸다.
     *
     * <p><b>정렬을 여기서 다시 하지 않는다.</b> 후보 질의가 {@code ORDER BY id} 로 주는 것이
     * 계약이고, 그 계약이 깨지면 연속부가 아니라 <b>중간이 뚫린 구간</b>이 나온다 —
     * {@code lastId} 밖에 같은 회차의 만료 대상이 남는데 {@code afterId} 는 그 위로 밀려
     * <b>그 건들이 영영 안 넘어간다.</b> 조용히 정렬해서 덮으면 그 계약이 깨진 사실도 덮인다.
     * 그래서 어긋나면 던진다.
     *
     * @param candidates {@code (id, couponId)} 를 id 오름차순으로. 빈 목록이면 {@link #EMPTY}
     */
    public static ExpireChunk from(List<ExpireCandidate> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return EMPTY;
        }
        long couponId = candidates.getFirst().couponId();
        long lastId = candidates.getFirst().id();
        int size = 1;
        for (int i = 1; i < candidates.size(); i++) {
            ExpireCandidate candidate = candidates.get(i);
            if (candidate.id() <= lastId) {
                throw new IllegalArgumentException(
                        "후보가 id 오름차순이 아닙니다. 연속부를 자를 수 없습니다 — "
                                + "이대로 두면 lastId 밖에 남은 같은 회차의 만료 대상이 "
                                + "afterId 아래로 묻혀 영영 안 넘어갑니다. "
                                + "앞=" + lastId + " 뒤=" + candidate.id());
            }
            if (candidate.couponId() != couponId) {
                break;
            }
            lastId = candidate.id();
            size++;
        }
        return new ExpireChunk(couponId, lastId, size);
    }

    /** 후보가 하나도 없다 — 잡을 끝낸다. */
    public boolean isEmpty() {
        return size == 0;
    }
}
