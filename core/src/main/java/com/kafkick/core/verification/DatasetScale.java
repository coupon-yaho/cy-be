// 한 검증 실행이 실제로 몇 건을 보았는지입니다.
package com.kafkick.core.verification;

/**
 * <b>{@code PASS 0건} 의 분모.</b> <i>"다 보고 못 찾았다"</i> 와 <i>"거의 아무것도 안 봤다"</i>
 * 를 가르는 수다.
 *
 * <p>{@code dataset_fingerprint} 의 재료와 <b>같은 질의에서 나온다</b> — 새로 세지 않는다.
 * 지문은 그 수들을 해시로 접어 <b>같음/다름</b>만 말하고, 이쪽은 <b>얼마인지</b>를 말한다.
 *
 * <p>⚠️ <b>판정에는 안 쓴다.</b> 처음에 <i>"검사 대상이 0이면 판정 불가"</i> 가드를
 * 넣으려다 철회했다 — 재고 불일치 규칙이 {@code coupons} 에서 시작해 발급건을
 * {@code LEFT JOIN} 하므로 <b>발급건 0에서도 검출을 낸다.</b> 재고가 5인데 발급이 0인
 * 상태가 바로 그 규칙이 잡아야 하는 사고라, 거기에 가드를 걸면 <b>진짜 검출을 덮는다.</b>
 * 실제로 그 가드를 넣었더니 기존 시험 셋이 깨졌고 그중 하나가 정확히 그 경우였다.
 *
 * @param issuanceCount 발급건 수. {@code issuances} 전수다
 * @param historyCount  {@code asOf} 까지의 이력 행 수. 리플레이가 보는 범위다
 */
public record DatasetScale(long issuanceCount, long historyCount) {

    public DatasetScale {
        if (issuanceCount < 0 || historyCount < 0) {
            throw new IllegalArgumentException(
                    "검사 규모는 음수일 수 없습니다. issuances=" + issuanceCount
                            + " histories=" + historyCount);
        }
    }
}
