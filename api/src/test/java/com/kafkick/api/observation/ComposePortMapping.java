package com.kafkick.api.observation;

import java.util.List;

/**
 * {@code compose.yml} 의 포트 매핑 문자열을 자른다.
 *
 * <p>따로 둔 이유 — <b>매핑 양쪽이 {@code ${VAR:-기본값}} 이 되면서 문자열 안의 콜론이
 * 구분자가 아니게 됐다.</b> {@code lastIndexOf(':')} 로 자르면
 * {@code "${API_HOST_PORT:-8080}:${SERVER_PORT:-8080}"} 이 {@code "-8080}"} 으로 잘린다.
 * 이 계약을 읽는 테스트가 둘이라(포트 뜻 분리 · 관리 포트 비노출) 각자 자르게 두면 한쪽만
 * 고쳐지고, 안 고쳐진 쪽은 "확인했는데 통과" 가 아니라 "엉뚱한 값을 확인" 이 된다.
 */
final class ComposePortMapping {

    /** {@code ${...}} 안쪽 콜론은 구분자가 아니다. */
    private static final String SEPARATOR = ":(?![^{]*})";

    private ComposePortMapping() {}

    /** {@code "127.0.0.1:${A}:8080"} → {@code [127.0.0.1, ${A}, 8080]}. */
    static List<String> split(String mapping) {
        return List.of(mapping.split(SEPARATOR));
    }

    /** {@code "${VAR:-8080}"} → {@code "8080"}. 변수가 아니면 그대로 돌려준다. */
    static String defaultOfFragment(String fragment) {
        if (!fragment.startsWith("${")) {
            return fragment;
        }
        String inside = fragment.substring(2, fragment.lastIndexOf('}'));
        int separator = inside.indexOf(":-");
        if (separator < 0) {
            throw new AssertionError("기본값 없는 플레이스홀더라 무엇이 열리는지 알 수 없다: " + fragment);
        }
        return inside.substring(separator + 2);
    }
}
