package com.kafkick.api.support;

import com.fasterxml.jackson.annotation.JsonProperty;

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
