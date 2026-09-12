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

    /**
     * <b>정규 인코딩만 받는다 — 형제 코덱 넷과 같은 규약이다.</b>
     *
     * <p>Base64 는 같은 바이트열을 여러 문자열로 쓸 수 있다. 마지막 바이트의 남는
     * 비트를 다르게 채워도 디코딩 결과가 같다 — 그대로 두면 <b>하나의 논리적 커서가
     * 여러 토큰으로 표현된다.</b>
     */
    @Test
    @DisplayName("비정규 인코딩은 거부한다 — 같은 커서가 여러 토큰이 되면 안 된다")
    void refusesANonCanonicalEncoding() {
        ResidualCursor cursor = new ResidualCursor(
                FindingType.DUP_PER_MEMBER, TargetKey.couponMember(1, 2));
        String canonical = cursor.encode();
        String tweaked = nonCanonicalVariantOf(canonical);

        assertThat(Base64.getUrlDecoder().decode(tweaked))
                .as("전제 — 같은 바이트열로 디코딩돼야 이 시험이 뜻이 있다")
                .isEqualTo(Base64.getUrlDecoder().decode(canonical));
        assertThatThrownBy(() -> ResidualCursor.decode(tweaked))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("패딩이 붙으면 거부한다")
    void refusesAPaddedToken() {
        String padded = Base64.getUrlEncoder()   // withoutPadding 이 아니다
                .encodeToString("DUP_PER_MEMBER\u001FCOUPON:1".getBytes(StandardCharsets.UTF_8));

        assertThat(padded).as("전제 — 패딩이 붙어야 한다").contains("=");
        assertThatThrownBy(() -> ResidualCursor.decode(padded))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>상한은 이 커서에서 유도한 값이다.</b> 가장 긴 {@code FindingType}(18자)과
     * 대상 키 상한(64자)으로 만든 토큰이 <b>111자</b>다 — 상한이 그보다 커야 하고,
     * 그 관계가 깨지면 <b>정상 커서가 거절된다.</b>
     */
    @Test
    @DisplayName("가장 긴 정상 커서가 상한 안에 들어온다")
    void theLongestLegitimateTokenFitsUnderTheCap() {
        String longestType = java.util.Arrays.stream(FindingType.values())
                .map(Enum::name)
                .max(java.util.Comparator.comparingInt(String::length))
                .orElseThrow();
        String longestKey = "I".repeat(TargetKey.MAX_LENGTH);
        String token = new ResidualCursor(FindingType.valueOf(longestType), longestKey).encode();

        assertThat(ResidualCursor.decode(token))
                .as("가장 긴 정상 커서가 거절되면 마지막 페이지를 못 넘긴다")
                .isNotNull();
        assertThat(token.length()).as("실측 111자").isLessThan(128);
    }

    /**
     * <b>상한이 없으면 이 토큰이 통과한다.</b> 모양은 완전히 정상이다 — 정규
     * Base64URL 이고, 구분자가 있고, 유형도 실재한다. <b>대상 키만 터무니없이 길다.</b>
     *
     * <p>그래서 나머지 가드로는 못 잡는다. {@code "A".repeat(400)} 같은 쓰레기는
     * 구분자 검사에서 어차피 걸려서 <b>길이 검사가 있든 없든 거절된다</b> —
     * 그것으로 시험하면 상한을 통째로 지워도 초록이다(실제로 그랬다).
     */
    @Test
    @DisplayName("모양은 멀쩡한데 대상 키만 긴 토큰을 상한이 잡는다")
    void refusesAWellFormedTokenThatIsTooLong() {
        String token = base64Url("DUP_PER_MEMBER" + SEPARATOR + "COUPON:" + "9".repeat(300));

        assertThat(token.length()).as("전제 — 상한을 넘어야 한다").isGreaterThan(128);
        assertThatThrownBy(() -> ResidualCursor.decode(token))
                .as("상한이 없으면 임의 길이 키가 그대로 커서가 된다")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("빈 토큰은 거부한다")
    void refusesABlankToken() {
        assertThatThrownBy(() -> ResidualCursor.decode("   "))
                .isInstanceOf(IllegalArgumentException.class);
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
     *
     * <p><b>그러면서 받은 값은 안 남긴다.</b> 이 메시지는 로그로 흘러가고 토큰은
     * 바깥에서 온 값이라, CR/LF 를 넣으면 로그 줄이 위조된다.
     */
    @Test
    @DisplayName("모르는 검출 종류는 거부한다")
    void refusesATokenWithAnUnknownFindingType() {
        String token = base64Url("V7_MADE_UP" + SEPARATOR + "COUPON:1");

        assertThatThrownBy(() -> ResidualCursor.decode(token))
                .isInstanceOf(IllegalArgumentException.class)
                .as("받은 값을 메시지에 실으면 로그 줄이 위조된다(CWE-117)")
                .hasMessageNotContaining("V7_MADE_UP");
    }

    /**
     * 같은 바이트열로 디코딩되는 <b>다른</b> 문자열을 만든다.
     *
     * <p>Base64 마지막 글자는 남는 비트를 싣는데, 디코더가 그 비트를 버린다.
     * 그래서 그 자리를 바꿔도 결과가 같다 — 정규 검사가 없으면 통과한다.
     */
    private static String nonCanonicalVariantOf(String canonical) {
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        byte[] want = Base64.getUrlDecoder().decode(canonical);
        String head = canonical.substring(0, canonical.length() - 1);
        for (char c : alphabet.toCharArray()) {
            String candidate = head + c;
            if (candidate.equals(canonical)) {
                continue;
            }
            try {
                if (java.util.Arrays.equals(Base64.getUrlDecoder().decode(candidate), want)) {
                    return candidate;
                }
            } catch (IllegalArgumentException ignored) {
                // 이 글자로는 디코딩이 안 된다. 다음 후보.
            }
        }
        throw new IllegalStateException("비정규 변형을 못 만들었습니다: " + canonical);
    }

    private static String base64Url(String raw) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }
}
