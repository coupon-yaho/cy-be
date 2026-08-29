package com.kafkick.api.support;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * 응답 헤더로 되반사하거나 로그에 남기는 <b>자유 형식 헤더</b> 값을 거른다.
 *
 * <p>배제하는 것은 <b>제어문자 · 공백 · 큰따옴표 · 역슬래시</b>와 128자 초과뿐이다.
 * {@code ' ; < > % &} 같은 문자는 통과한다 — 헤더 분리와 로그 위조를 막는 게 목적이지
 * SQL·HTML 문맥의 안전을 보장하지 않는다. 그런 문맥에서는 각자 이스케이프해야 한다.
 *
 * <p>형식이 정해진 헤더는 이걸 쓰지 말고 그 형식으로 직접 파싱한다. 예를 들어
 * {@code X-User-Id} 는 {@code long} 으로 파싱한다.
 */
public final class HeaderValues {

    private static final int MAX_LENGTH = 128;

    private static final Pattern SAFE =
            Pattern.compile("[\\x21-\\x7E&&[^\"\\\\]]{1," + MAX_LENGTH + "}");

    private HeaderValues() {
    }

    /** 앞뒤 공백을 제거한 뒤 안전하면 그 값을, 아니면 비어 있음을 돌려준다. */
    public static Optional<String> safe(String raw) {
        if (raw == null) {
            return Optional.empty();
        }
        String trimmed = raw.trim();
        return SAFE.matcher(trimmed).matches() ? Optional.of(trimmed) : Optional.empty();
    }
}
