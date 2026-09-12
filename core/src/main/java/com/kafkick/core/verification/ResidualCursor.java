// 대상 목록을 이어 받을 자리입니다.
package com.kafkick.core.verification;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * <b>페이지를 이어받는 자리.</b> 정렬 키 두 개를 그대로 들고 다닌다.
 *
 * <h2>오프셋이 아니라 커서인 이유</h2>
 *
 * <p><b>{@code LIMIT} 이 이 질의의 비용을 안 줄인다(실측).</b> 집합을 만드는 {@code GROUP BY}
 * 가 임시 테이블로 전체를 만든 뒤에야 자르기 때문에, 100건을 뽑아도 12만 키 형상에서
 * 220ms 가 나온다 — 전량(270ms)과 큰 차이가 없다.
 *
 * <p>그래서 커서를 <b>파생 테이블 안쪽</b>으로 밀어 {@code GROUP BY} 대상 자체를 줄인다.
 * 그때 비로소 뒤 페이지가 싸진다(맨 앞 237ms · 중간 52ms · 거의 끝 19ms).
 * <b>바깥에 붙이면 {@code LIMIT} 과 똑같이 아무것도 안 줄인다.</b>
 *
 * <h2>왜 그룹이 안 쪼개지나</h2>
 *
 * <p>커서 튜플이 {@code GROUP BY} 키와 <b>같은 축</b>이라, 한 그룹은 통째로 들어오거나
 * 통째로 빠진다. 다른 축으로 자르면 같은 대상의 앞뒤 실행이 서로 다른 페이지로 갈려
 * <b>{@code PERSISTED} 가 {@code INTRODUCED} 와 {@code RESOLVED} 둘로 보인다.</b>
 *
 * @param type 이 유형까지 봤다
 * @param targetKey 그 유형 안에서 이 키까지 봤다
 */
public record ResidualCursor(FindingType type, String targetKey) {

    public ResidualCursor {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(targetKey, "targetKey");
    }

    /**
     * 값 사이 구분자. 대상 키에 <b>절대 안 나오는</b> 제어문자여야 한다 —
     * {@link TargetKey} 는 {@code :} 와 {@code |} 를 쓴다.
     *
     * <p>같은 이유로 {@code findings_checksum} 도 U+001F 를 쓴다.
     */
    private static final char UNIT_SEPARATOR = '\u001F';

    /**
     * 토큰 길이 상한. <b>형제에서 베낀 256 이 아니라 이 커서에서 유도한 값이다.</b>
     *
     * <p>싣는 것은 {@code 유형 이름 + 구분자 + 대상 키} 뿐이고 둘 다 상한이 있다 —
     * 가장 긴 {@code FindingType} 은 {@code ILLEGAL_TRANSITION}(18자),
     * 대상 키는 {@link TargetKey#MAX_LENGTH}(64자)다. 최악 83바이트이고 그 Base64URL
     * 이 <b>111자</b>다(실측). 128 은 그 위로 한 자릿수 여유를 둔 자리다.
     *
     * <p><b>왜 상한이 필요한가</b> — 없으면 임의 길이 문자열이 그대로
     * {@code Base64.decode} 로 간다. 형제 코덱 넷이 모두 상한을 거는 이유이고,
     * 여기만 안 걸 이유가 없다.
     */
    private static final int MAX_TOKEN_LENGTH = 128;

    /** 방금 돌려준 마지막 줄에서 다음 커서를 만든다. */
    public static ResidualCursor after(ResidualTarget last) {
        Objects.requireNonNull(last, "last");
        return new ResidualCursor(last.type(), last.targetKey());
    }

    /**
     * <b>URL 에 그대로 실을 수 있는 한 덩어리로 만든다.</b>
     *
     * <h2>왜 두 값을 그냥 안 싣나 — 재 보고 알았다</h2>
     *
     * <p>{@code DUP_PER_MEMBER} 의 대상 키는 {@code COUPON:1|MEMBER:2} 다
     * ({@link TargetKey#couponMember}). <b>{@code |} 는 톰캣이 요청 타깃에서 거부한다</b>
     * (기본값 {@code relaxedQueryChars} 미설정 — 이 저장소에 그 설정이 없다).
     * 게다가 그 유형은 이름 순서가 <b>맨 앞</b>이라 첫 페이지의 커서에 바로 들어간다.
     *
     * <p>거부는 <b>컨트롤러에 닿기 전</b>에 일어나므로 {@code BatchApiExceptionHandler}
     * 를 안 탄다 — 봉투도 {@code VERIFICATION-027} 도 아닌 <b>스프링 기본 본문</b>이
     * 나가고, 운영자가 보는 것은 원인 불명 400 이다.
     *
     * <p><i>"인코딩해서 보내라"</i> 고 문서에 적는 길도 있었지만, 그러면 <b>잊는 쪽이
     * 지는 계약</b>이 된다. Base64URL 알파벳({@code A-Za-z0-9-_})은 질의 문자열에서
     * 그대로 안전하므로 부르는 쪽이 아무것도 안 해도 된다.
     *
     * <p>덤으로 <b>반쪽 커서가 불가능해진다.</b> 두 파라미터로 두면 하나만 보내는
     * 경우를 막는 가드가 따로 필요했다.
     */
    public String encode() {
        String raw = type.name() + UNIT_SEPARATOR + targetKey;
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * {@link #encode()} 를 되돌린다.
     *
     * <p><b>불투명하다고 안 따지는 것이 아니다.</b> 손으로 지어낸 토큰이 조용히
     * <i>"처음부터"</i> 로 접히면 부르는 쪽은 이어받은 줄 알고 <b>앞 페이지를 다시
     * 처리한다</b> — 같은 대상에 조치를 두 번 넣는 길이다.
     *
     * <p><b>메시지에 받은 값을 안 싣는다.</b> 이 예외의 메시지는 로그로 흘러가는데
     * 토큰은 바깥에서 온 값이다 — CR/LF 를 넣으면 로그 줄이 위조된다(CWE-117).
     * 무엇이 틀렸는지는 <b>종류</b>로만 말한다.
     *
     * <p><b>형제 코덱 넷과 같은 방어를 한다</b> — 길이 상한 · 패딩 거부 ·
     * <b>정규 인코딩만</b>. 셋째가 핵심이다: 안 하면 같은 커서가 여러 토큰으로
     * 표현된다. CY-958 이 그 규약을 뒤늦게 맞췄다.
     *
     * @throws IllegalArgumentException 토큰이 비었거나, 상한을 넘거나, 패딩이 있거나,
     *         Base64URL 이 아니거나, 정규 인코딩이 아니거나, 구분자가 없거나,
     *         검출 종류가 이 시스템의 값이 아닐 때
     */
    public static ResidualCursor decode(String token) {
        Objects.requireNonNull(token, "token");
        if (token.isBlank() || token.length() > MAX_TOKEN_LENGTH || token.indexOf('=') >= 0) {
            // 형제 코덱 넷과 같은 순서다 — 길이·패딩을 먼저 보고 디코딩으로 간다.
            throw new IllegalArgumentException("커서의 형식이 올바르지 않습니다.");
        }
        byte[] bytes;
        try {
            bytes = Base64.getUrlDecoder().decode(token);
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException(
                    "커서가 Base64URL 이 아닙니다. 응답의 nextCursor 를 그대로 실어 보내십시오.",
                    malformed);
        }
        if (!Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(token)) {
            // **정규 인코딩만 받는다.** Base64 는 같은 바이트열을 여러 문자열로 쓸 수
            // 있다(패딩 유무, 마지막 바이트의 남는 비트). 그대로 두면 **하나의 논리적
            // 커서가 여러 토큰으로 표현된다** — 형제 코덱 넷이 모두 막는 자리다.
            throw new IllegalArgumentException("커서가 정규 Base64URL 인코딩이 아닙니다.");
        }
        String raw = new String(bytes, StandardCharsets.UTF_8);
        int at = raw.indexOf(UNIT_SEPARATOR);
        if (at < 0 || at == raw.length() - 1) {
            throw new IllegalArgumentException("커서에 검출 종류와 대상 키가 다 있어야 합니다.");
        }
        try {
            return new ResidualCursor(
                    FindingType.valueOf(raw.substring(0, at)), raw.substring(at + 1));
        } catch (IllegalArgumentException unknown) {
            // ⚠️ **받은 값을 메시지에 안 싣는다.** 이 예외의 메시지는 부르는 쪽에서
            // BusinessException.detail 이 되고, BatchApiExceptionHandler 가 그것을
            // `detail={}` 로 로그에 남긴다. 토큰은 질의 문자열에서 온 **바깥 값**이라
            // CR/LF 를 넣으면 로그 줄을 위조할 수 있다(CWE-117).
            // 그 자리 주석이 스스로 적어 뒀다 — detail 에는 설정 키·가드 이름·실행 id
            // 가 들어간다고. 즉 **안쪽 값만** 넣는 자리다.
            throw new IllegalArgumentException("커서의 검출 종류를 모릅니다.", unknown);
        }
    }
}
