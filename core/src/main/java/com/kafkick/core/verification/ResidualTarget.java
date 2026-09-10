// 전후 비교에서 한 대상이 어느 유형으로 어느 쪽에 있었는지입니다.
package com.kafkick.core.verification;

import java.util.Objects;

/**
 * <b>{@code (유형, 대상)} 한 줄.</b> 집계가 접어 버린 그레인이다.
 *
 * <p><b>{@code expected}·{@code actual} 을 안 싣는다.</b> 그 둘은 자유 문자열이라
 * 포맷이 한 글자만 달라도 같은 대상이 다르게 보인다 — {@code findings_checksum} 이
 * 같은 이유로 그 둘을 뺐다. 여기 실린 두 값이 집합 비교의 키 전부다.
 *
 * @param type 검출 규칙
 * @param targetKey 정규화 키. 형식은 {@link TargetKey} 가 소유한다
 * @param kind 앞뒤 중 어느 쪽에 있었는지
 */
public record ResidualTarget(FindingType type, String targetKey, ResidualKind kind) {

    public ResidualTarget {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(kind, "kind");
        if (targetKey == null || targetKey.isBlank()) {
            throw new IllegalArgumentException("targetKey 는 비어 있을 수 없습니다.");
        }
        if (targetKey.length() > TargetKey.MAX_LENGTH) {
            throw new IllegalArgumentException(
                    "targetKey 가 컬럼 길이를 넘습니다. 상한=" + TargetKey.MAX_LENGTH
                            + " 받은 길이=" + targetKey.length());
        }
    }
}
