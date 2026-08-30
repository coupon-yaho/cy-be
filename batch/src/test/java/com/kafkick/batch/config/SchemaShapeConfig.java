// 만료 배치가 보고 있는 스키마 모양을 테스트가 갈아 끼웁니다.
package com.kafkick.batch.config;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import com.kafkick.core.verification.VerificationRuleRepository;

/**
 * {@code hasCleanOnlyConstraints} 만 덮는다. 나머지 호출은 실제 저장소로 흘려보낸다.
 *
 * <p><b>인덱스를 실제로 떼지 않는 것이 결정이다.</b> 공용 컨테이너를 여러 테스트가 나눠
 * 쓰므로, {@code uk_coupon_member} 를 지웠다가 되돌리는 방식은 되돌리기 전에 테스트가 죽는
 * 날 <b>다른 테스트를 조용히 오염시킨다.</b>
 *
 * <p><b>두 테스트가 각자 갖고 있던 것을 모았다.</b> {@code CleanSchemaGuardTest} 는 가드가
 * 만료를 멈추는지를, {@code ExpireMetricExposureTest} 는 그렇게 멈춘 실행이 지표에 무엇을
 * 남기는지를 본다 — 축은 다르지만 <b>스키마를 갈아 끼우는 방법은 같다.</b> 두 벌로 두면
 * 판정 근거를 바꾸는 날 한쪽만 고쳐진다.
 *
 * <p>이 저장소가 같은 이유로 모아 둔 것이 {@code ExpirationProxies}·{@code JobFailures}·
 * {@code ActuatorProbe} 다.
 *
 * <p><b>모으면서 상태가 클래스 경계를 넘었다.</b> 각자 갖고 있을 때는 {@code static} 이
 * 그 클래스 안에 갇혀 있었는데, 이제는 한 테스트가 남긴 <i>오염</i> 이 다음 클래스로 샌다.
 * 그래서 리셋을 쓰는 쪽 규율에 맡기지 않고 <b>{@link BeforeEachCallback} 으로 이 클래스가
 * 진다</b> — {@code @ExtendWith} 만 붙이면 빠뜨릴 수가 없다. 리셋을 잊으면 오염 단언이
 * {@link #pretendCorrupt()} 없이도 통과해 <b>가드를 안 지키는 테스트가 초록이 된다.</b>
 */
@TestConfiguration
public class SchemaShapeConfig implements BeforeEachCallback {

    private static volatile boolean corrupt;

    /** 테스트마다 정상 스키마에서 시작한다. */
    @Override
    public void beforeEach(ExtensionContext context) {
        corrupt = false;
    }

    public static void pretendCorrupt() {
        corrupt = true;
    }

    public static void pretendClean() {
        corrupt = false;
    }

    @Bean
    @Primary
    VerificationRuleRepository schemaShapeOverride(
            @Qualifier("verificationRuleJdbcAdapter") VerificationRuleRepository real) {
        return (VerificationRuleRepository) Proxy.newProxyInstance(
                VerificationRuleRepository.class.getClassLoader(),
                new Class<?>[] {VerificationRuleRepository.class},
                (proxy, method, args) -> {
                    if ("hasCleanOnlyConstraints".equals(method.getName())) {
                        return !corrupt;
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
