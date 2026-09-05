package com.kafkick.core.batch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 실패 원문을 응답에 실을 수 있는 만큼만 줄인다.
 *
 * <p>배치 메타의 EXIT_MESSAGE 와 Step 의 exitDescription 에는 스택트레이스가 통째로
 * 들어간다(실측 2,178자). 첫 줄에도 SQL 조각·드라이버 오류·제약 이름이 섞인다.
 * <p>앞에 무엇이 서 있는지는 구성으로 갈린다. 표준 스택은 <b>포트 미노출</b>이 전부라
 * 토큰 관문이 꺼져 있고(batch.yml 의 BATCH_ADMIN_AUTH_REQUIRED 기본값 false), 포트를
 * 내보내는 batch-expose.yml 만 그것을 켠다(CY-742). 즉 <b>둘 중 하나는 항상 없다.</b>
 * 켜진 쪽도 공유 비밀 하나라 TLS 없이 평문으로 오가고 회수 수단이 재기동뿐이다.
 * 그래서 이 자리는 앞단을 안 믿고 <b>언제나</b> 줄인다.
 *
 * <p>한 곳에 둔 이유는 호출부마다 복사하면 반드시 갈리기 때문이다 — 실제로 갈렸다.
 * VerifyRunView 와 BatchRunView 가 같은 EXIT_MESSAGE 에 다른 답을 냈다.
 */
public final class FailureSummary {

    /**
     * 우리가 정의한 도메인 에러코드. 접두사를 열거한다.
     *
     * <p>{@code [A-Z]+-\d{3}} 로 넓히면 오탐한다 — {@code ISO-8859-1} 에서 {@code ISO-885} 를
     * 잡고, find() 는 첫 매치를 쓰므로 뒤에 있는 진짜 코드가 가려진다.
     * 이 저장소에는 {@code SHA-256} 도 있어서 같은 함정에 걸린다(실측).
     *
     * <p><b>접두사가 한 낱말이 아니다.</b> 두 모양이 섞여 있다 —
     * 밑줄({@code COUPON_ROUND}·{@code COUPON_TEMPLATE}·{@code RUNTIME_CONFIG})과
     * 하이픈({@code ADMIN-INQUIRY}·{@code ADMIN-COUPON-ROUND}).
     *
     * <p><b>둘 다 처음에 빠뜨렸고 둘 다 리뷰가 잡았다.</b> 밑줄은 {@code [A-Z]+-} 로 세다가,
     * 하이픈은 그 다음 판에서 {@code [A-Z][A-Z_]*-} 로 세다가 놓쳤다 — 세는 정규식이
     * <b>알아보는 정규식과 같은 맹점</b>을 가지고 있어서, 검사가 조용히 통과했다.
     * 그래서 {@link FailureSummaryPrefixContractTest} 의 수집 정규식은 이제 두 모양을
     * 모두 받는다. 놓치면 그 코드가 <b>예외 이름으로 뭉개져</b> 화면에서 사라진다.
     *
     * <p><b>교대 순서는 긴 것부터다.</b> {@code ADMIN} 이 {@code ADMIN-INQUIRY} 를 가리지
     * 않도록 — 역추적이 있어 순서가 없어도 맞지만, 읽는 사람에게 의도를 남긴다.
     *
     * <p><b>앞뒤에 경계가 있다.</b> 없으면 부분 일치한다 — 실측이다:
     *
     * <pre>
     *   NOTADMIN-001 실패  -&gt;  ADMIN-001     (경계 없을 때)
     *   XCOUPON-002        -&gt;  COUPON-002
     * </pre>
     *
     * <p><b>이 예시에 따옴표를 안 쓴다.</b> {@link FailureSummaryPrefixContractTest} 가
     * 본코드의 <b>문자열 리터럴</b>을 훑어 접두사를 모으는데, 주석 속 예시도 따옴표가
     * 있으면 진짜 에러코드로 걷힌다 — 실제로 {@code XCOUPON} 이 그렇게 걸렸다.
     *
     * <p>{@code find()} 는 <b>문자열 어디서든</b> 찾으므로 접두사가 낱말 가운데 있어도
     * 걸린다. 실패 원문은 스택트레이스라 <b>클래스 이름·패키지 조각이 잔뜩 섞여 있고</b>,
     * 그중 하나가 우연히 우리 접두사로 끝나면 <b>없는 에러코드가 화면에 뜬다.</b>
     * 리뷰가 잡았다.
     *
     * <p>앞 경계는 <b>유니코드 글자</b>까지 본다({@code \p{L}}) — ASCII 만 막으면
     * 한국어 낱말이 바로 앞에 붙은 경우를 놓친다.
     *
     * <p>뒤 경계는 숫자만 막는다({@code (?![0-9])}) — 세 자리로 정의된 코드이므로 네 자리는
     * 우리 것이 아니다. 뒤에 글자가 오는 것은 막지 않는다: {@code COMMON-001입니다} 처럼
     * 조사가 붙는 한국어 문장이 실제로 온다.
     */
    private static final Pattern DOMAIN_CODE = Pattern.compile(
            // 앞 경계. 영숫자·밑줄·하이픈이 붙어 있으면 그 코드가 아니다 — 아래 설명.
            "(?<![\\p{L}\\p{N}_-])"
                    + "(?:ADMIN-COUPON-ROUND|ADMIN-INQUIRY|ADMIN"
                    + "|ANALYTICS|BENCHMARK|COMMON|CONSISTENCY"
                    + "|COUPON_ROUND|COUPON_TEMPLATE|COUPON"
                    + "|EXPIRATION|NOTIFY|OVERVIEW|RUNTIME_CONFIG|VERIFICATION)-\\d{3}"
                    // 뒤 경계. 네 자리 이상이면 우리 코드가 아니다(예: ISO-8859-1).
                    + "(?![0-9])");

    /** 그 밖에는 예외 이름만 남긴다. 메시지에는 SQL 조각이 섞일 수 있다. */
    private static final Pattern EXCEPTION_TYPE =
            Pattern.compile("([A-Za-z]+(?:Exception|Error))");

    public static final String NOT_RECORDED = "원인이 기록되지 않았습니다";
    public static final String UNKNOWN = "알 수 없는 오류";

    private FailureSummary() {
    }

    /**
     * 도메인 에러코드가 보이면 그것을, 아니면 예외 클래스 이름만.
     *
     * <p>못 알아본 것과 원인이 없는 것을 문구로 가른다. 둘 다 null 로 접으면 화면이
     * "성공이라 원인이 없다" 로 읽어 서버 로그를 열어야 한다는 사실 자체가 안 보인다.
     */
    public static String of(String rawMessage) {
        if (rawMessage == null || rawMessage.isBlank()) {
            return NOT_RECORDED;
        }
        Matcher code = DOMAIN_CODE.matcher(rawMessage);
        if (code.find()) {
            return code.group();
        }
        Matcher type = EXCEPTION_TYPE.matcher(rawMessage);
        return type.find() ? type.group(1) : UNKNOWN;
    }
}
