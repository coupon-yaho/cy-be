package com.kafkick.core.support.response;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 성공·실패를 같은 봉투로 감싼다. HTTP status 는 실제 4xx/5xx 를 유지한다.
 *
 * <p><b>{@code core} 에 있는 이유는 {@code batch} 도 이것을 써야 하기 때문이다.</b>
 * 원래 {@code api} 모듈에 있었는데, CY-368 이 batch 에 컨트롤러를 열면서 같은 규약을
 * 두 벌로 만들 뻔했다 — 같은 사실을 두 곳이 각자 정의하면 언젠가 어긋난다.
 * batch 는 {@code core} 만 의존하므로 여기가 유일한 공유 지점이다.
 *
 * <p>컴포넌트 이름을 {@code success} 로 바꿔 {@code @JsonProperty} 를 없애려 했지만
 * <b>정적 팩토리 {@code success(T)} 와 accessor 가 충돌</b>한다. 그래서 애노테이션을
 * 유지하고 {@code core} 에 {@code jackson-annotations} 만 더했다 — 직렬화 구현이 아니라
 * 애노테이션 jar 하나라, 도메인 모듈이 무거워지지 않는다.
 */
public record ResponseEnvelope<T>(
        @JsonProperty("success") boolean isSuccess,
        T data,
        ErrorResponse error
) {
    public static <T> ResponseEnvelope<T> success(T data) {
        return new ResponseEnvelope<>(true, data, null);
    }

    public static ResponseEnvelope<Void> success() {
        return new ResponseEnvelope<>(true, null, null);
    }

    public static ResponseEnvelope<Void> fail(ErrorResponse error) {
        return new ResponseEnvelope<>(false, null, error);
    }
}
