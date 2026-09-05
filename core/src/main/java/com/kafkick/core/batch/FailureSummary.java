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
     */
    private static final Pattern DOMAIN_CODE = Pattern.compile(
            "(?:ADMIN|ANALYTICS|BENCHMARK|COMMON|CONSISTENCY|COUPON"
                    + "|EXPIRATION|NOTIFY|OVERVIEW|VERIFICATION)-\\d{3}");

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
