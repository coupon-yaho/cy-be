package com.kafkick.api.support.lock;

import java.util.List;

/**
 * {@link LockContentionRetry} 의 {@code operation} 태그로 나갈 값 전부.
 *
 * <p><b>지표 태그라 값의 가짓수가 고정이어야 한다.</b> 호출부가 문자열을 즉석에서 만들면
 * 오타 하나가 새 시계열을 만들고, 요청에서 온 값이 섞이면 태그가 무한히 늘어난다.
 * 그래서 여기 적힌 것만 쓴다.
 */
public final class LockRetryOperations {

    public static final String ISSUE = "issue";
    public static final String USE = "use";
    public static final String CANCEL_USE = "cancel-use";
    public static final String CANCEL = "cancel";

    /**
     * 전부. <b>{@link LockRetryMeters} 가 이걸 훑어 카운터를 미리 등록한다</b> — 안 그러면
     * 한 번도 안 부딪힌 경로가 대시보드에서 0 이 아니라 "데이터 없음" 으로 나온다.
     * 새 경로를 넣으면 여기에도 넣는다.
     */
    public static final List<String> ALL = List.of(ISSUE, USE, CANCEL_USE, CANCEL);

    /**
     * {@code outcome} 태그로 나갈 값 전부. <b>셋이라야 합이 맞는다</b> — 물러선 요청은
     * 성공(recovered)·소진(exhausted) 말고 <b>다른 실패로 끝나는 셋째 결말</b>이 있다.
     * 사용·취소에서는 그게 드문 일도 아니다: 경합에서 진 뒤 다시 하면 상대가 이미 상태를
     * 바꿔 놨을 수 있고, 그때는 락 경합이 아니라 전이 거절로 끝난다.
     */
    public static final String RECOVERED = "recovered";
    public static final String EXHAUSTED = "exhausted";
    public static final String ABANDONED = "abandoned";

    /** 위 셋 전부. 미터가 이걸 훑어 경로마다 미리 등록한다. */
    public static final List<String> OUTCOMES = List.of(RECOVERED, EXHAUSTED, ABANDONED);

    private LockRetryOperations() {
    }
}
