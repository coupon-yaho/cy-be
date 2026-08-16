// 검출 결과 한 행입니다. verification_findings 로 그대로 들어갑니다.
package com.kafkick.core.verification;

/**
 * <b>{@code targetKey} 를 직접 만들지 못하게 막습니다.</b> 규칙마다 키 형식이 다른데
 * 형식이 어긋나면 개수는 맞고 키만 달라져, 시드가 기록한 {@code expected_findings} 와
 * 양방향 MINUS 에서 <b>누락 N · 오탐 N</b> 으로 동시에 잡힙니다. 원인을 찾기가 가장 어려운 형태입니다.
 * 그래서 정식 생성자와 grain 별 정적 팩토리가 <b>같은 판별기</b>({@link TargetKey#matches})로
 * 검사합니다. 팩토리에만 검사를 두면 정식 생성자로 우회됩니다 — {@code record} 의 정식 생성자는
 * public 이라 자동완성이 그것을 먼저 제안합니다.
 *
 * <p><b>레거시 컬럼 이름에 주의합니다.</b> 어휘가 뒤집혀 있어 필드와 컬럼이 1:1 로 안 읽힙니다.
 *
 * <pre>
 * couponId    → campaign_id   회차 (coupons.id)
 * issuanceId  → coupon_id     발급건 (issuances.id)
 * </pre>
 *
 * <p>개별 FK 컬럼은 조회 편의로만 둡니다. 집합 비교·UNIQUE·checksum 은 전부
 * {@code targetKey} 로만 돕니다 — 다형 컬럼으로 조인하면 {@code NULL = NULL} 이 UNKNOWN 이라
 * 정확히 검출한 finding 이 전부 누락으로 뒤집힙니다.
 */
public record VerificationFinding(
        FindingType type,
        String targetKey,
        Long couponId,
        Long memberId,
        Long issuanceId,
        Long historyId,
        String expected,
        String actual
) {

    /** {@code expected} · {@code actual} 이 varchar(200) 이고 둘 다 NOT NULL 입니다. */
    public static final int EVIDENCE_MAX_LENGTH = 200;

    public VerificationFinding {
        if (type == null) {
            throw new IllegalArgumentException("검출 규칙이 필요합니다.");
        }
        // 정식 생성자도 막는다. 팩토리에만 검사가 있으면 IDE 자동완성이 그것을 그냥 지나친다.
        if (!TargetKey.matches(type.grain(), targetKey)) {
            throw new IllegalArgumentException(
                    "규칙의 검출 단위와 키 형식이 다릅니다. 규칙=" + type
                            + " 단위=" + type.grain() + " 키=" + targetKey);
        }
        validateEvidence(expected, "기대값");
        validateEvidence(actual, "실제값");
    }

    /** V1 재고 정합 — 회차 단위 */
    public static VerificationFinding forCoupon(
            FindingType type, long couponId, String expected, String actual) {
        requireGrain(type, FindingType.Grain.COUPON);

        return new VerificationFinding(
                type, TargetKey.coupon(couponId), couponId, null, null, null, expected, actual);
    }

    /** V2 1인 1매·발급코드 중복 — (회차, 회원) 단위 */
    public static VerificationFinding forCouponMember(
            FindingType type, long couponId, long memberId, String expected, String actual) {
        requireGrain(type, FindingType.Grain.COUPON_MEMBER);

        return new VerificationFinding(
                type, TargetKey.couponMember(couponId, memberId),
                couponId, memberId, null, null, expected, actual);
    }

    /** V3 리플레이 대조 · V5 사용 실적 · V6 등급 자격 — 발급건 단위 */
    public static VerificationFinding forIssuance(
            FindingType type, long issuanceId, String expected, String actual) {
        requireGrain(type, FindingType.Grain.ISSUANCE);

        return new VerificationFinding(
                type, TargetKey.issuance(issuanceId),
                null, null, issuanceId, null, expected, actual);
    }

    /** V4 불법 전이 — 이력 행 단위 */
    public static VerificationFinding forHistory(
            FindingType type, long historyId, String expected, String actual) {
        requireGrain(type, FindingType.Grain.HISTORY);

        return new VerificationFinding(
                type, TargetKey.history(historyId),
                null, null, null, historyId, expected, actual);
    }

    private static void requireGrain(FindingType type, FindingType.Grain expected) {
        if (type == null) {
            throw new IllegalArgumentException("검출 규칙이 필요합니다.");
        }
        if (type.grain() != expected) {
            throw new IllegalArgumentException(
                    "규칙의 검출 단위와 키 형식이 다릅니다. 규칙=" + type
                            + " 단위=" + type.grain() + " 만들려는 키=" + expected);
        }
    }

    private static void validateEvidence(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    name + "이 필요합니다. 없으면 리포트가 무엇이 이상한지까지만 말합니다.");
        }
        if (value.length() > EVIDENCE_MAX_LENGTH) {
            throw new IllegalArgumentException(
                    name + "은 " + EVIDENCE_MAX_LENGTH + "자를 넘을 수 없습니다. 길이=" + value.length());
        }
    }
}
