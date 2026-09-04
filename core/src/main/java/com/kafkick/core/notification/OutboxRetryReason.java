package com.kafkick.core.notification;

/**
 * 발행 명령을 <b>왜</b> 되돌리는가.
 *
 * <h2>왜 문자열이 아니라 enum 인가</h2>
 *
 * <p>이 값이 지표 태그가 된다. 문자열로 두면 오타 하나가 <b>대시보드에도 알림에도 안 잡히는
 * 새 시계열</b>을 만들고, 그것을 막으려면 런타임 검사를 따로 둬야 한다. enum 이면 컴파일이
 * 막으므로 검사도 그 검사의 테스트도 필요 없다.
 *
 * <h2>왜 {@code core} 인가</h2>
 *
 * <p>사유를 <b>아는 쪽</b>과 결과를 <b>아는 쪽</b>이 다른 모듈이다. 릴레이({@code infra:mq})만
 * 왜 실패했는지 알고, 저장소 어댑터({@code storage})만 그 쓰기가 먹었는지·상한을 넘겼는지
 * 안다. 둘이 만나야 지표가 맞으므로 사유가 포트를 타고 넘어간다.
 */
public enum OutboxRetryReason {

    /** 발행이 던졌다. */
    PUBLISH_FAILED("publish_failed"),

    /** 발행 대상 알림이 사라졌다. */
    NOTIFICATION_MISSING("notification_missing"),

    /** 잡고 있던 워커가 lease 안에 못 끝냈다. 저장소 어댑터가 스스로 붙인다. */
    LEASE_EXPIRED("lease_expired");

    private final String tag;

    OutboxRetryReason(String tag) {
        this.tag = tag;
    }

    /** 지표 태그 값. <b>이름을 바꾸면 대시보드가 끊긴다</b> — enum 상수명과 따로 둔 이유다. */
    public String tag() {
        return tag;
    }
}
