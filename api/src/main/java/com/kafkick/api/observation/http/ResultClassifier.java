package com.kafkick.api.observation.http;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

        /**
         * 실패 <b>비율</b>의 분자가 되는 분류입니다.
         *
         * <p><b>실패 전체가 아닙니다.</b> {@link #POLICY_REJECT} 와 {@link #CLIENT_INVALID} 는
         * 시스템이 정상 동작한 결과라 분자에 넣으면 정책상 거절이 장애로 보입니다.</p>
         *
         * <p>이 정의가 화면 여러 곳에 흩어지면 스냅샷({@code /metrics})과 추세선
         * ({@code /metrics/series})의 숫자가 조용히 갈립니다. 분류가 늘어날 때 여기만 고치면
         * 되도록 한 곳에 둡니다.</p>
         *
         * @return 시스템 책임 실패로 세는 분류들
         */
        public static Set<ResultClass> systemFailures() {
            return SYSTEM_FAILURES;
        }

        private static final Set<ResultClass> SYSTEM_FAILURES =
                Collections.unmodifiableSet(EnumSet.of(DEPENDENCY_FAILURE, APPLICATION_FAILURE));

        /** @return 이 분류들의 Prometheus {@code result} 라벨 값 정규식 대안 */
        public static String promLabelAlternation(Set<ResultClass> classes) {
            return classes.stream()
                    .map(value -> value.name().toLowerCase(Locale.ROOT))
                    .sorted()
                    .collect(Collectors.joining("|"));
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
