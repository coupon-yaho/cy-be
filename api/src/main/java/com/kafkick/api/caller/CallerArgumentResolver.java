package com.kafkick.api.caller;

import java.util.Optional;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/**
 * 컨트롤러가 {@code Caller} 를 파라미터로 받게 한다.
 *
 * <p>이게 없으면 각 컨트롤러가 {@code HttpServletRequest} 에서 헤더를 다시 꺼내게 되고
 * 헤더 이름과 검증 규칙이 여러 곳으로 흩어진다.
 *
 * <p>선언 방식이 곧 필수 여부다 — {@code Caller} 는 없으면 400,
 * {@code Optional<Caller>} 는 없어도 통과. <b>{@code null} 을 조용히 주입하지 않는다.</b>
 */
@Component
public class CallerArgumentResolver implements HandlerMethodArgumentResolver {

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return Caller.class.equals(parameter.getParameterType())
                || Caller.class.equals(optionalValueType(parameter));
    }

    @Override
    public Object resolveArgument(
            MethodParameter parameter, ModelAndViewContainer mavContainer,
            NativeWebRequest webRequest, WebDataBinderFactory binderFactory) {

        Optional<Caller> caller = Optional.ofNullable(
                webRequest.getNativeRequest(HttpServletRequest.class))
                .map(request -> (Caller) request.getAttribute(CallerFilter.ATTRIBUTE));

        if (Caller.class.equals(optionalValueType(parameter))) {
            return caller;
        }
        return caller.orElseThrow(() -> new BusinessException(CommonErrorCode.INVALID_INPUT));
    }

    private static Class<?> optionalValueType(MethodParameter parameter) {
        if (!Optional.class.equals(parameter.getParameterType())) {
            return null;
        }
        return ResolvableType.forMethodParameter(parameter).getGeneric(0).resolve();
    }
}
