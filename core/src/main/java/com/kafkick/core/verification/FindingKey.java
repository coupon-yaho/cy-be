// 검출과 정답을 대조하는 단위입니다. 두 컬럼뿐인 것이 계약입니다.
package com.kafkick.core.verification;

/**
 * <b>다형 FK 로 조인하지 않는 이유가 여기 있습니다.</b> {@code verification_findings} 와
 * {@code expected_findings} 는 {@code campaign_id}·{@code member_id}·{@code coupon_id}·
 * {@code history_id} 를 <b>규칙마다 다르게</b> 채웁니다 — V1 은 회차만, V4 는 이력만 씁니다.
 * 그 컬럼으로 조인하면 안 쓰는 쪽이 양쪽 다 NULL 이고, SQL 에서 {@code NULL = NULL} 은
 * UNKNOWN 이라 <b>정확히 검출한 행까지 전부 누락으로 뒤집힙니다.</b>
 *
 * <p>그래서 대조는 {@code (finding_type, target_key)} 두 컬럼으로만 합니다.
 * 시드의 참조 구현({@code cy-seed/seedgen/verify.py#compare_with_expected})도 같은 두 값의
 * 집합 차를 씁니다.
 */
public record FindingKey(String findingType, String targetKey) {

    public FindingKey {
        if (findingType == null || findingType.isBlank()) {
            throw new IllegalArgumentException("검출 종류가 필요합니다.");
        }
        if (targetKey == null || targetKey.isBlank()) {
            throw new IllegalArgumentException("검출 대상 키가 필요합니다.");
        }
    }

    @Override
    public String toString() {
        return findingType + ":" + targetKey;
    }
}
