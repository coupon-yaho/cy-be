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

    /**
     * <b>발행이 던졌다.</b> 외부로 나가지 않았다.
     *
     * <p>⚠️ <b>기록 실패를 여기 섞지 말 것.</b> 한때 {@code try} 블록이 발행과 기록을 둘 다
     * 감싸서, {@code markPublished} 가 던져도 이 사유가 붙었다 — <b>발행은 성공했는데</b>
     * 지표는 발행 실패로 셌다. 그러면 운영자가 이름을 보고 <b>카프카를 뒤지는데 문제는
     * DB</b> 다. 그 자리가 {@link #RECORD_FAILED} 다.
     */
    PUBLISH_FAILED("publish_failed"),

    /**
     * <b>발행은 됐는데 그 사실을 기록하지 못했다.</b>
     *
     * <p>외부로는 이미 나갔다. 그래서 되돌려 다시 집는 것은 <b>재발행</b>이고, 그것은
     * 소비자가 흡수한다 — 상태·계보 검사와 멱등 키가 세 겹으로 막는다(실측으로 확인).
     *
     * <p>그 키는 {@code notification.id() + ":" + baseAttemptSeq} 다 —
     * <b>{@code attemptSeq} 가 아니라 {@code baseAttemptSeq}</b> 이고, 그 차이가 여기서
     * 결정적이다. 자동 재시도는 {@code attemptSeq} 를 올리지만 같은 논리적 발송이라,
     * 그것으로 키를 만들면 <b>재시도마다 키가 바뀌어 두 번 발송된다</b>
     * ({@code NotificationDeliveryDecision} 이 그 이유를 적어 뒀다).
     *
     * <p><b>그럼에도 따로 세는 이유</b> — 이 사유로 되돌아간 건은 <b>이미 나간</b> 건인데
     * {@code failure_count} 는 똑같이 오른다. 열 번이면 그 명령은 {@code DEAD} 로 가서
     * <i>"지금 사람 손이 필요한 건수"</i> 에 오르는데 <b>그 알림은 나갔다.</b>
     * {@link #PUBLISH_FAILED} 와 뭉치면 그 사실이 안 보인다.
     *
     * <p>⚠️ <b>열 번이 다 이 사유라는 뜻은 아니다.</b> {@code markFailed} 는
     * {@code reason} 을 안 보고 세므로 앞선 시도는 다른 사유였을 수 있다 —
     * 종착에 붙는 사유는 <b>마지막 실패</b>의 것이다.
     *
     * <p>사전예약 명세가 같은 축을 적었다 — <i>"외부 성공 후 응답 유실 … 응답이 없다는
     * 이유만으로 미등록 또는 최종 실패 단정"</i>(§5.3).
     */
    RECORD_FAILED("record_failed"),

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
