// 만료 저장소를 프록시로 감싸는 배선을 모읍니다.
package com.kafkick.batch.job;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.springframework.beans.factory.config.BeanPostProcessor;

import com.kafkick.core.expiration.ExpirationRepository;

/**
 * <b>같은 15줄이 네 테스트에 복붙돼 있었다.</b> {@code instanceof} 검사 · {@code newProxyInstance} ·
 * {@code InvocationTargetException} 평치기 — 각자 다른 것은 인터셉터 본문 몇 줄뿐이었다.
 *
 * <p>이 저장소는 같은 상황에서 도구를 모아 왔다. {@link JobFailures} 는 <i>"파싱 방식을 바꿔야
 * 할 때 한쪽만 고치면 나머지가 조용히 옛 방식으로 남는다"</i> 고 적었고,
 * {@code ActuatorProbe} 도 같은 이유로 만들었다.
 *
 * <p><b>그리고 복붙되던 그 부분이 정확히 깨지기 쉬운 자리다.</b>
 * {@code catch (InvocationTargetException e) { throw e.getCause(); }} 를 한 번 빠뜨리면
 * 프록시가 {@code UndeclaredThrowableException} 을 던져서, 잡은 제대로 실패했는데
 * <b>메시지가 안 맞아</b> 단언이 깨진다 — 원인이 프록시 배선이라는 것은 로그에 안 나온다.
 */
final class ExpirationProxies {

    private ExpirationProxies() {
    }

    /** 저장소 호출 하나를 가로챈다. {@code real} 을 부를지 말지는 구현이 정한다. */
    @FunctionalInterface
    interface Around {
        Object invoke(ExpirationRepository real, Method method, Object[] args) throws Throwable;
    }

    /**
     * 원본을 그대로 부른다. 감싸는 쪽이 <b>결과만</b> 손대거나 <b>호출을 관찰만</b> 할 때 쓴다.
     *
     * <p>{@code InvocationTargetException} 을 벗겨서 던지는 것이 핵심이다 — 그래야 잡이
     * {@code BusinessException} 을 원래 모습으로 받고, {@link JobFailures} 가 평치는 원인
     * 체인에 그 메시지가 들어온다.
     */
    static Object callThrough(ExpirationRepository real, Method method, Object[] args)
            throws Throwable {
        try {
            return method.invoke(real, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /**
     * {@code ExpirationRepository} 빈만 골라 {@code around} 로 감싼다.
     *
     * <p>{@code @Bean static} 으로 등록해야 한다 — {@code BeanPostProcessor} 는 다른 빈보다
     * 먼저 만들어져야 하고, 인스턴스 메서드로 두면 그 설정 클래스가 조기 초기화된다.
     */
    static BeanPostProcessor decorating(Around around) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) {
                if (!(bean instanceof ExpirationRepository real)) {
                    return bean;
                }
                return Proxy.newProxyInstance(
                        ExpirationRepository.class.getClassLoader(),
                        new Class<?>[] {ExpirationRepository.class},
                        (proxy, method, args) -> around.invoke(real, method, args));
            }
        };
    }
}
