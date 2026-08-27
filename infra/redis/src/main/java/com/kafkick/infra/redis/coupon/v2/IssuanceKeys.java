package com.kafkick.infra.redis.coupon.v2;

/**
 * 회차 하나의 v2 키 네 개. <b>키 이름과 해시태그의 출처는 여기 한 곳이다.</b>
 *
 * <p>리터럴을 호출부·테스트에 흩뿌리면 그중 하나만 바뀐 상태가 초록으로 남는다. 특히
 * 정합성 리더는 설정으로 키를 받으므로(11 문서 ③), 어긋난 사실은 부하 시험 끝에 gap 4축이
 * 안 닫힐 때에야 드러난다.
 *
 * <p><b>해시태그 {@code {회차}} 는 선택이 아니다.</b> 네 키가 같은 슬롯에 떨어져야 하나의 Lua 가
 * 그것들을 함께 만질 수 있다. Cluster 로 가는 시점에 붙이면 그때는 이미 스크립트가 전부
 * CROSSSLOT 이다. 그래서 키를 만드는 길 자체를 이 클래스 하나로 좁힌다.
 */
public final class IssuanceKeys {

    private static final String PREFIX = "cy:v2:";

    private final String hashTag;

    private IssuanceKeys(long couponRoundId) {
        this.hashTag = "{" + couponRoundId + "}";
    }

    public static IssuanceKeys of(long couponRoundId) {
        return new IssuanceKeys(couponRoundId);
    }

    /** 잔여 재고. {@code DECR}·{@code INCR} 대상이라 canonical 정수 문자열이다. */
    public String stock() {
        return key("stock");
    }

    /** 발급자 Hash. field=memberId, value=4필드 codec(01 문서). */
    public String issued() {
        return key("issued");
    }

    /** 게이트 데이터 Hash. 다섯 필드다. */
    public String meta() {
        return key("meta");
    }

    /** 누적 발급 수. 취소로 줄지 않는다. */
    public String issuedEver() {
        return key("issued_ever");
    }

    private String key(String name) {
        return PREFIX + name + ":" + hashTag;
    }
}
