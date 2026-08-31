package com.kafkick.core.coupon.v2.port;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 선점 인자의 불변식. <b>우리가 만든 값과 클라이언트가 준 값을 다르게 다룬다</b> — 전자는
 * 어기면 호출부 버그라 그 자리에서 터뜨리고, 후자는 잘못된 요청이라 입력 검증의 몫이다.
 */
class ClaimCommandTest {

    private static final String TOKEN = "api-1-n1-t1-1";
    private static final String KEY = "idem-1";

    @Test
    @DisplayName("'|' 가 든 토큰은 만들 수 없다 — 필드 경계가 밀려 보상이 남의 선점을 되돌린다")
    void rejectsTokenWithDelimiter() {
        assertThatThrownBy(() -> new ClaimCommand(7, 42, 1, KEY, "api|1-n1-t1-1"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("빈 토큰도 만들 수 없다 — 저장된 토큰과 같아질 수 없어 비교가 아니라 버그다")
    void rejectsEmptyToken() {
        assertThatThrownBy(() -> new ClaimCommand(7, 42, 1, KEY, ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("멱등키는 여기서 막지 않는다 — 클라이언트 값이라 빈 값의 거부는 입력 검증(400)이다")
    void keepsIdempotencyKeyOpaque() {
        assertThatCode(() -> new ClaimCommand(7, 42, 1, "", TOKEN)).doesNotThrowAnyException();
        // '|' 와 개행이 든 키는 정상이다(01). 여기서 막으면 Java 만 파손으로 세어 축이 갈린다.
        assertThat(new ClaimCommand(7, 42, 1, "order|1\ntrace-2", TOKEN).idempotencyKey())
                .isEqualTo("order|1\ntrace-2");
    }

    @Test
    @DisplayName("음수 등급 비트는 스크립트가 -10 으로 돌려주기 전에 막는다")
    void rejectsNegativeGradeBit() {
        assertThatThrownBy(() -> new ClaimCommand(7, 42, -1, KEY, TOKEN))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
