// 커서 토큰이 왕복하고, 지어낸 토큰을 조용히 접지 않는지입니다.
package com.kafkick.core.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>토큰이 불투명하다고 안 따지는 것이 아니다.</b>
 *
 * <p>손으로 지어낸 토큰이 조용히 <i>"처음부터"</i> 로 접히면 부르는 쪽은 이어받은 줄
 * 알고 <b>앞 페이지를 다시 처리한다</b> — 같은 대상에 조치를 두 번 넣는 길이다.
 *
 * <p>HTTP 쪽 시험({@code VerifyReportApiTest})은 <b>Base64 로 안 읽히는</b> 토큰
 * 하나만 태운다. 그 앞 검사에서 튕기므로 <b>구조 검증 두 분기</b>(구분자 없음 ·
 * 모르는 유형)는 거기서 안 태워진다 — 여기가 그 자리다.
 */
class ResidualCursorTest {

    /** {@link ResidualCursor} 안쪽과 같은 구분자. 대상 키에 절대 안 나오는 제어문자다. */
    private static final String SEPARATOR = "\u001F";

    @Test
    @DisplayName("인코딩한 것을 그대로 되읽는다")
    void aTokenRoundTrips() {
        ResidualCursor cursor = new ResidualCursor(
                FindingType.DUP_PER_MEMBER, TargetKey.couponMember(1, 2));

        assertThat(ResidualCursor.decode(cursor.encode())).isEqualTo(cursor);
    }

    /**
     * <b>질의 문자열에 그대로 실을 수 있어야 한다.</b> 이 성질이 무너지면 톰캣이
     * 컨트롤러에 닿기 전에 요청을 끊는다 — 봉투도 에러코드도 없이 400 이 나간다.
     */
    @Test
    @DisplayName("토큰은 Base64URL 알파벳뿐이다")
    void aTokenCarriesNoCharacterThatNeedsEncoding() {
        String token = new ResidualCursor(
                FindingType.DUP_PER_MEMBER, TargetKey.couponMember(1, 2)).encode();

        assertThat(token)
                .as("`|` 나 `+`·`/`·`=` 가 섞이면 URL 에 그대로 못 싣는다")
                .matches("[A-Za-z0-9_-]+");
    }

    @Test
    @DisplayName("Base64URL 이 아니면 거부한다")
    void refusesATokenThatIsNotBase64() {
        assertThatThrownBy(() -> ResidualCursor.decode("not-a-real-cursor"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>구분자가 없으면 거부한다.</b> 이 분기를 지우면 {@code substring} 이
     * {@code StringIndexOutOfBoundsException} 을 던지는데, 그것은
     * {@code IllegalArgumentException} 이 <b>아니라</b> 부르는 쪽 catch 를 빠져나가
     * <b>500</b> 이 된다.
     */
    @Test
    @DisplayName("구분자가 없는 토큰은 거부한다 — 500 이 아니라")
    void refusesATokenWithoutTheSeparator() {
        String token = base64Url("hello");

        assertThatThrownBy(() -> ResidualCursor.decode(token))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 구분자가 <b>맨 끝</b>이면 대상 키가 빈 문자열이다 — 그것도 커서가 아니다. */
    @Test
    @DisplayName("대상 키가 비면 거부한다")
    void refusesATokenWithAnEmptyTargetKey() {
        String token = base64Url("DUP_PER_MEMBER" + SEPARATOR);

        assertThatThrownBy(() -> ResidualCursor.decode(token))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * 모르는 유형이면 거부한다. 조용히 접으면 그 커서가 <b>어느 자리도 안 가리키는데</b>
     * 페이지는 정상으로 보인다.
     */
    @Test
    @DisplayName("모르는 검출 종류는 거부한다")
    void refusesATokenWithAnUnknownFindingType() {
        String token = base64Url("V7_MADE_UP" + SEPARATOR + "COUPON:1");

        assertThatThrownBy(() -> ResidualCursor.decode(token))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("V7_MADE_UP");
    }

    private static String base64Url(String raw) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }
}
