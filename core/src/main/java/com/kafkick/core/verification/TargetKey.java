// 검출 대상을 가리키는 정규화 키입니다. 집합 비교·UNIQUE·checksum 이 전부 이 문자열로만 이뤄집니다.
package com.kafkick.core.verification;

/**
 * <b>다형 FK 컬럼으로 조인하면 안 됩니다.</b> 규칙마다 채우는 컬럼이 달라 나머지는 NULL 인데
 * MySQL 에서 {@code NULL = NULL} 은 참이 아니라 UNKNOWN 입니다. 그대로 비교하면
 * <b>정확히 검출한 finding 이 전부 "누락"으로 뒤집힙니다.</b>
 * campaign_id · member_id · coupon_id · history_id 는 조회 편의로만 남깁니다.
 *
 * <pre>
 * V1        COUPON:{coupons.id}
 * V2        COUPON:{coupons.id}|MEMBER:{members.id}
 * V3 V5 V6  ISSUANCE:{issuances.id}
 * V4        HISTORY:{issuance_histories.id}
 * </pre>
 *
 * <p><b>어휘 주의.</b> {@code coupons} 는 쿠폰이 아니라 <b>회차</b>(147행)이고 발급건은
 * {@code issuances}(300만)입니다. 구 어휘의 {@code CAMPAIGN:} · {@code COUPON:{발급건}} 을 쓰면
 * 시드가 기록한 expected_findings 와 100% 어긋나, 개수는 맞는데 키가 달라 원인을 못 찾습니다.
 */
public final class TargetKey {

    /** verification_findings.target_key · expected_findings.target_key 가 varchar(64) 입니다. */
    public static final int MAX_LENGTH = 64;

    private static final String COUPON_PREFIX = "COUPON:";
    private static final String MEMBER_SEPARATOR = "|MEMBER:";
    private static final String ISSUANCE_PREFIX = "ISSUANCE:";
    private static final String HISTORY_PREFIX = "HISTORY:";

    private TargetKey() {
    }

    /** V1 재고 정합 — 회차 단위 */
    public static String coupon(long couponId) {
        validateId(couponId, "회차 ID");

        return checked(COUPON_PREFIX + couponId);
    }

    /** V2 1인 1매·발급코드 중복 — (회차, 회원) 단위 */
    public static String couponMember(long couponId, long memberId) {
        validateId(couponId, "회차 ID");
        validateId(memberId, "회원 ID");

        return checked(COUPON_PREFIX + couponId + MEMBER_SEPARATOR + memberId);
    }

    /** V3 리플레이 대조 · V5 사용 실적 · V6 등급 자격 — 발급건 단위 */
    public static String issuance(long issuanceId) {
        validateId(issuanceId, "발급건 ID");

        return checked(ISSUANCE_PREFIX + issuanceId);
    }

    /** V4 불법 전이 — 이력 행 단위 */
    public static String history(long historyId) {
        validateId(historyId, "이력 ID");

        return checked(HISTORY_PREFIX + historyId);
    }

    /**
     * 컬럼이 {@code varchar(64)} 다. 넘으면 MySQL 이 잘라 저장하고, 잘린 키는 정답과 안 맞아
     * <b>누락 N · 오탐 N 이 동시에</b> 뜬다 — 가장 찾기 어려운 형태다.
     *
     * <p>지금 형식으로는 넘을 수 없다(최장 조합이 53자). 형식이 바뀌는 순간을 위한 것이다.
     */
    private static String checked(String key) {
        if (key.length() > MAX_LENGTH) {
            throw new IllegalArgumentException(
                    "검출 키가 " + MAX_LENGTH + "자를 넘습니다. 길이=" + key.length() + " 키=" + key);
        }
        return key;
    }

    private static void validateId(long id, String name) {
        if (id <= 0) {
            throw new IllegalArgumentException(
                    name + "는 0보다 커야 합니다."
            );
        }
    }
}
