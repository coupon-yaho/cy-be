package com.kafkick.api.observation;

import com.kafkick.api.observation.issuance.IssuanceObservationService;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 발급 관측의 운영 임계치를 외부 설정에서 읽습니다.
 *
 * <p>{@code @Value}가 아니라 {@link ConfigurationProperties}를 쓰는 이유 — {@code @Value}는
 * 문자열 {@code "10s"}를 {@link Duration}으로 바꾸지 못해 기동이 실패합니다(실측). 바인딩
 * 경로가 달라서이며, 같은 이유로 {@code ConsistencySeverityProperties}도 이 방식입니다.
 *
 * @param attemptFailureLogInterval 발급 시도 기록 실패를 다시 로그로 남기기까지의 최소 간격;
 *                                  생략하면 {@link IssuanceObservationService#DEFAULT_LOG_INTERVAL}
 * @param producerInstanceId 이벤트를 만든 API 인스턴스 식별자; 생략하면 로컬 기본값 사용
 * @param replayPendingRetryAfterSeconds v2 {@code -7}(처리 중) 응답의 {@code Retry-After} 초;
 *                                       생략하면 {@link #DEFAULT_RETRY_AFTER_SECONDS}
 * @param gateNotReadyRetryAfterSeconds v2 {@code -9}(재구성 창) 응답의 {@code Retry-After} 초;
 *                                      생략하면 {@link #DEFAULT_RETRY_AFTER_SECONDS}
 */
@ConfigurationProperties(prefix = "observation.issuance")
public record ObservationIssuanceProperties(
        Duration attemptFailureLogInterval,
        String producerInstanceId,
        Integer replayPendingRetryAfterSeconds,
        Integer gateNotReadyRetryAfterSeconds
) {

    private static final String DEFAULT_PRODUCER_INSTANCE_ID = "api-local";
    /**
     * 스파이크에서 재시도가 한 초 격자에 정렬되면 두 번째 봉우리가 된다. 그때 늘리는 값이다.
     *
     * <p><b>반영에는 api 재기동이 필요하다.</b> 이 저장소에 런타임 갱신 장치는 없고
     * ({@code @RefreshScope} 사용처 0곳) 회차 도중에 바꿔야 하는 값만
     * {@code RuntimeConfigStore} 에 있다 — 엔진·릴리스 단계·대기열 모드 셋이다. 이 값은
     * 그쪽이 아니다. 재시도 간격을 바꾸면 그 회차 측정은 어차피 버린다.
     *
     * <p>외부화가 사는 지점은 <b>이미지가 그대로라는 것</b>이다. 코드를 고쳐 다시 빌드하면
     * 그 회차는 "코드가 바뀐 회차" 라 튜닝 효과와 코드 변경 효과가 섞인다.
     */
    public static final int DEFAULT_RETRY_AFTER_SECONDS = 1;

    public ObservationIssuanceProperties {
        producerInstanceId = producerInstanceId == null
                ? DEFAULT_PRODUCER_INSTANCE_ID
                : producerInstanceId;
        if (producerInstanceId.isBlank()
                || producerInstanceId.length() > 100) {
            throw new IllegalArgumentException(
                    "producerInstanceId는 1자 이상 100자 이하여야 합니다."
            );
        }
        replayPendingRetryAfterSeconds =
                requirePositiveSeconds(replayPendingRetryAfterSeconds, "replayPendingRetryAfterSeconds");
        gateNotReadyRetryAfterSeconds =
                requirePositiveSeconds(gateNotReadyRetryAfterSeconds, "gateNotReadyRetryAfterSeconds");
    }

    /**
     * 0 이하를 막는다. {@code Retry-After: 0} 은 "즉시 다시 보내라" 라서 재시도 폭주와 같다.
     *
     * @param seconds 설정값. {@code null} 이면 기본값
     * @param name 오류 메시지에 넣을 설정 이름
     * @return 실제로 쓸 초
     */
    private static Integer requirePositiveSeconds(Integer seconds, String name) {
        if (seconds == null) {
            return DEFAULT_RETRY_AFTER_SECONDS;
        }
        if (seconds <= 0) {
            throw new IllegalArgumentException(name + "는 1 이상이어야 합니다.");
        }
        return seconds;
    }

    /**
     * 설정값이 없으면 기본 간격을 돌려줍니다.
     *
     * @return 실제로 사용할 실패 로그 유량 제한 간격
     */
    public Duration resolvedAttemptFailureLogInterval() {
        return attemptFailureLogInterval == null
                ? IssuanceObservationService.DEFAULT_LOG_INTERVAL
                : attemptFailureLogInterval;
    }
}
