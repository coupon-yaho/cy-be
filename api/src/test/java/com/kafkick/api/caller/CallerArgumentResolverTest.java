package com.kafkick.api.caller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.core.MethodParameter;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.ServletWebRequest;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

class CallerArgumentResolverTest {

    private final CallerArgumentResolver resolver = new CallerArgumentResolver();

    /** 시그니처를 읽기 위한 대상. 호출하지 않는다. */
    @SuppressWarnings("unused")
    static class Handlers {
        void required(Caller caller) {
        }

        void optional(Optional<Caller> caller) {
        }

        void unrelated(String other) {
        }
    }

    private static MethodParameter parameterOf(String method, Class<?> type) throws Exception {
        return new MethodParameter(Handlers.class.getDeclaredMethod(method, type), 0);
    }

    private Object resolve(MethodParameter parameter, Caller present) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        if (present != null) {
            request.setAttribute(CallerFilter.ATTRIBUTE, present);
        }
        return resolver.resolveArgument(parameter, null, new ServletWebRequest(request), null);
    }

    @Test
    @DisplayName("Caller 와 Optional<Caller> 만 지원한다")
    void supportsOnlyCallerTypes() throws Exception {
        assertThat(resolver.supportsParameter(parameterOf("required", Caller.class))).isTrue();
        assertThat(resolver.supportsParameter(parameterOf("optional", Optional.class))).isTrue();
        assertThat(resolver.supportsParameter(parameterOf("unrelated", String.class))).isFalse();
    }

    @Test
    @DisplayName("Caller 로 선언했는데 헤더가 없으면 400 — null 을 조용히 주입하지 않는다")
    void requiredWithoutHeaderFails() throws Exception {
        MethodParameter parameter = parameterOf("required", Caller.class);

        assertThatThrownBy(() -> resolve(parameter, null))
                .isInstanceOf(BusinessException.class)
                .extracting(e -> ((BusinessException) e).getErrorCode())
                .isEqualTo(CommonErrorCode.INVALID_INPUT);
    }

    @Test
    @DisplayName("Caller 로 선언하고 헤더가 있으면 그대로 준다")
    void requiredWithHeader() throws Exception {
        assertThat(resolve(parameterOf("required", Caller.class), new Caller(812934L)))
                .isEqualTo(new Caller(812934L));
    }

    @Test
    @DisplayName("Optional<Caller> 는 헤더가 없어도 통과한다")
    void optionalWithoutHeader() throws Exception {
        assertThat(resolve(parameterOf("optional", Optional.class), null))
                .isEqualTo(Optional.empty());
    }

    @Test
    @DisplayName("Optional<Caller> 는 헤더가 있으면 담아서 준다")
    void optionalWithHeader() throws Exception {
        assertThat(resolve(parameterOf("optional", Optional.class), new Caller(812934L)))
                .isEqualTo(Optional.of(new Caller(812934L)));
    }
}
