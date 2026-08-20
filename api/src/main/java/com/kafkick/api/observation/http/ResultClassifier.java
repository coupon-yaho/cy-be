package com.kafkick.api.observation.http;

import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.observation.Dependency;

/** HTTP 결과를 서로 겹치지 않는 운영 결과 여섯 종류로 분류한다. */
@Component
public final class ResultClassifier {

    public enum ResultClass {
        SUCCESS(true),
        QUEUE_ACCEPTED(true),
        POLICY_REJECT(false),
        CLIENT_INVALID(false),
        DEPENDENCY_FAILURE(false),
        APPLICATION_FAILURE(false);

        private final boolean success;

        ResultClass(boolean success) {
            this.success = success;
        }

        public boolean isSuccess() {
            return success;
        }
    }

    /**
     * 분류에는 I/O가 없으며 여섯 분류의 경계는 HTTP 상태와 dependency로 완전히 결정된다.
     */
    public ResultClass classify(int httpStatus, Dependency dependency) {
        Objects.requireNonNull(dependency, "dependency");
        if (httpStatus < 100 || httpStatus > 599) {
            throw new IllegalArgumentException("invalid HTTP status: " + httpStatus);
        }

        if (httpStatus == 200 || httpStatus == 201) {
            return ResultClass.SUCCESS;
        }
        if (httpStatus == 202) {
            return ResultClass.QUEUE_ACCEPTED;
        }
        if (httpStatus == 403 || httpStatus == 409) {
            return ResultClass.POLICY_REJECT;
        }
        // 인증·라우팅·멱등키 충돌(422)·rate limit을 포함한 나머지 4xx는 요청 계약 위반이다.
        if (httpStatus >= 400 && httpStatus < 500) {
            return ResultClass.CLIENT_INVALID;
        }
        if (httpStatus >= 500) {
            return dependency == Dependency.NONE
                    ? ResultClass.APPLICATION_FAILURE
                    : ResultClass.DEPENDENCY_FAILURE;
        }
        return ResultClass.CLIENT_INVALID;
    }
}
