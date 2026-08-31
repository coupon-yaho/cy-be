package com.kafkick.core.coupon.exception;

import java.util.Optional;

import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.support.exception.ErrorCode;

/**
 * v2 게이트만 낼 수 있는 반환의 오류 코드.
 *
 * <p>{@code -1} ~ {@code -5} 는 v1 과 같은 판정이라 {@link CouponIssueErrorCode} 를 그대로 쓴다.
 * 여기 있는 것은 <b>게이트가 생겨서 처음 존재하게 된 결과</b>뿐이다.
 *
 * <p>{@link #REPLAY_PENDING} 을 {@link CouponIssueErrorCode#ALREADY_ISSUED} 로 접으면
 * 재시도한 클라이언트에게 "이미 받으셨습니다" 가 나간다 — 멱등이 아니라 고장이다.
 *
 * <p><b>재시도를 권하는 문구는 기다리면 상태가 바뀔 수 있는 코드에만 쓴다.</b>
 * {@link #REPLAY_PENDING}·{@link #GATE_NOT_READY}·{@link #REDIS_UNAVAILABLE}만
 * {@code Retry-After}를 단다. 마지막 것은 <b>요청 안에서</b> Redis 명령을 재시도하지 않는다는
 * 뜻이지 클라이언트 재시도까지 금지한다는 뜻은 아니다. 나머지는 다시 눌러도 같은 실패라 권하지 않는다.
 */
public enum CouponIssueV2ErrorCode implements ErrorCode {

    /** {@code -7}. 같은 멱등키가 아직 처리 중이다. 서버는 기다리지 않는다. */
    REPLAY_PENDING(
            409, "COUPON-320", "요청을 처리 중입니다. 잠시 후 다시 시도해 주세요.",
            ReasonCode.REPLAY_IN_PROGRESS, Dependency.NONE
    ),
    /** {@code -8}. 값 파손. 회수는 재구성 절차의 몫이라 응답 경로에서 부르지 않는다. */
    VALUE_CORRUPT(
            500, "COUPON-321", "요청을 처리하지 못했습니다.",
            ReasonCode.VALUE_CORRUPT, Dependency.REDIS
    ),
    /** {@code -9}. 게이트 미준비 — 재구성 창이면 기다려서 풀린다. */
    GATE_NOT_READY(
            503, "COUPON-322", "잠시 후 다시 시도해 주세요.",
            ReasonCode.GATE_NOT_READY, Dependency.REDIS
    ),
    /**
     * {@code -10}. 인자 이상. 호출부 버그이므로 의존성은 {@link Dependency#NONE} 이다 —
     * Redis 로 집계하면 멀쩡한 Redis 가 장애로 보인다.
     */
    BAD_ARGUMENT(
            500, "COUPON-323", "요청을 처리하지 못했습니다.",
            ReasonCode.BAD_ARGUMENT, Dependency.NONE
    ),
    /**
     * {@code -11}. 카운터를 못 읽는다. <b>매진이 아니다</b>. 기다려서 풀리지 않으므로
     * {@link #GATE_NOT_READY} 와 달리 재시도를 권하지 않는다.
     */
    COUNTER_UNREADABLE(
            503, "COUPON-324", "요청을 처리하지 못했습니다.",
            ReasonCode.COUNTER_UNREADABLE, Dependency.REDIS
    ),
    /** Redis failover·차단기 개방. 앞선 선점은 PENDING일 수 있어 서버는 재시도하지 않는다. */
    REDIS_UNAVAILABLE(
            503, "COUPON-325", "발급 시스템을 전환 중입니다. 잠시 후 다시 시도해 주세요.",
            ReasonCode.TEMPORARILY_UNAVAILABLE, Dependency.REDIS
    );

    private final int status;
    private final String code;
    private final String message;
    private final ReasonCode reasonCode;
    private final Dependency dependency;

    CouponIssueV2ErrorCode(
            int status,
            String code,
            String message,
            ReasonCode reasonCode,
            Dependency dependency
    ) {
        this.status = status;
        this.code = code;
        this.message = message;
        this.reasonCode = reasonCode;
        this.dependency = dependency;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public Optional<ReasonCode> reasonCode() {
        return Optional.of(reasonCode);
    }

    @Override
    public Dependency dependency() {
        return dependency;
    }

    /**
     * <b>의존성 장애 구간에 대량으로 나가는 5xx 는 스택을 남기지 않는다.</b>
     *
     * <p>{@link #REDIS_UNAVAILABLE} 은 정의상 failover·차단기 개방 구간 전용이라 그 1초에
     * 인플라이트 전량이 이 코드로 나간다. 요청마다 스택을 찍으면 로그 I/O 가 응답 지연을
     * 밀어 올려, 이 작업이 증명하려는 <b>failover 복구 시간 측정치 자체를 오염시킨다.</b>
     * {@link #GATE_NOT_READY} 와 {@link #COUNTER_UNREADABLE} 도 재구성 창에서 같은 성격이다.
     * 원인은 {@code GlobalExceptionHandler} 의 한 줄 요약 갈래가 {@code cause} 로 남긴다.
     *
     * <p>{@link #VALUE_CORRUPT} 와 {@link #BAD_ARGUMENT} 는 <b>남긴다</b> — 값 파손과 호출부
     * 버그는 드물고, 스택 없이는 어느 자리가 깨졌는지 추적할 수 없다.
     */
    @Override
    public boolean logStackTrace() {
        return this != REDIS_UNAVAILABLE
                && this != GATE_NOT_READY
                && this != COUNTER_UNREADABLE;
    }
}
