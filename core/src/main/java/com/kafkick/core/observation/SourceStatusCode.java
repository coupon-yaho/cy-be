package com.kafkick.core.observation;

/**
 * 원천 상태를 상태 Gauge 가 실을 수 있는 숫자로 바꾼다.
 *
 * <p>{@code ordinal()} 을 쓰지 않는 이유 — enum 에 상수를 하나 끼워 넣는 순간 이미 저장된
 * 시계열의 의미가 통째로 밀린다. 지난 회차 그래프가 조용히 다른 상태로 읽히므로 코드를 고정한다.
 * 새 상태는 항상 마지막 번호를 받는다.
 */
public final class SourceStatusCode {

    private SourceStatusCode() {
    }

    /**
     * 발급 엔진 버전을 지표가 실을 수 있는 숫자로 바꾼다. 상태 코드와 같은 이유로 {@code ordinal()}
     * 을 쓰지 않는다 — 상수가 하나 끼어들면 지난 회차 그래프의 의미가 밀린다.
     *
     * @param engineVersion 변환할 엔진 버전
     * @return V1=1 · V2=2 · V3=3
     */
    /**
     * 심각도를 지표가 실을 수 있는 숫자로 바꾼다.
     *
     * @param severity 변환할 심각도
     * @return NONE=0 · WARN=1 · CRITICAL=2
     */
    public static int of(Severity severity) {
        return switch (severity) {
            case NONE -> 0;
            case WARN -> 1;
            case CRITICAL -> 2;
        };
    }

    public static int of(EngineVersion engineVersion) {
        return switch (engineVersion) {
            case V1 -> 1;
            case V2 -> 2;
            case V3 -> 3;
        };
    }

    /**
     * 상태를 고정 코드로 변환한다.
     *
     * @param status 변환할 원천 상태
     * @return 1부터 시작하는 고정 코드
     */
    public static int of(SourceStatus status) {
        return switch (status) {
            case VALID -> 1;
            case PENDING -> 2;
            case WARMING_UP -> 3;
            case STALE -> 4;
            case NO_TRAFFIC -> 5;
            case UNAVAILABLE -> 6;
            case N_A -> 7;
        };
    }

    /**
     * 상태 Gauge 가 실어 보낸 고정 코드를 상태로 되돌린다. {@link #of(SourceStatus)} 의 역이며
     * 두 방향이 어긋나면 조회 쪽이 조용히 다른 상태로 읽으므로 왕복은 테스트가 지킨다.
     *
     * @param code 상태 Gauge 값
     * @return 코드에 대응하는 상태
     * @throws IllegalArgumentException 정의되지 않은 코드인 경우
     */
    public static SourceStatus statusOf(int code) {
        return switch (code) {
            case 1 -> SourceStatus.VALID;
            case 2 -> SourceStatus.PENDING;
            case 3 -> SourceStatus.WARMING_UP;
            case 4 -> SourceStatus.STALE;
            case 5 -> SourceStatus.NO_TRAFFIC;
            case 6 -> SourceStatus.UNAVAILABLE;
            case 7 -> SourceStatus.N_A;
            default -> throw new IllegalArgumentException("정의되지 않은 상태 코드입니다: " + code);
        };
    }

    /**
     * 심각도 Gauge 값을 심각도로 되돌린다. {@link #of(Severity)} 의 역이다.
     *
     * @param code 심각도 Gauge 값
     * @return 코드에 대응하는 심각도
     * @throws IllegalArgumentException 정의되지 않은 코드인 경우
     */
    public static Severity severityOf(int code) {
        return switch (code) {
            case 0 -> Severity.NONE;
            case 1 -> Severity.WARN;
            case 2 -> Severity.CRITICAL;
            default -> throw new IllegalArgumentException("정의되지 않은 심각도 코드입니다: " + code);
        };
    }
}
