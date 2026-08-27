package com.kafkick.api.observation.resource;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;

import javax.sql.DataSource;

import com.kafkick.api.observation.ApiObservationAutoConfiguration;
import com.kafkick.storage.db.config.MainDataSourceConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;

/**
 * "무엇을 재는가" 는 <b>두 파일에 걸친 계약</b>이라 여기서 잇는다.
 *
 * <ul>
 *   <li>{@code MainDataSourceConfig} — 운영 풀 빈의 <b>이름</b>을 정한다
 *       (CY-338 이전에는 {@code ObservationDataSourceConfig} 였다. 관측 스위치가 배치
 *       메타까지 끄는 결함 때문에 운영 풀만 떼어 냈다 — 빈 이름은 그대로다)
 *   <li>{@code ApiObservationAutoConfiguration} — 그 이름으로 잴 풀을 <b>지목</b>한다
 * </ul>
 *
 * <p>이름이 어긋나면 조건이 먼저 막아 빈이 조용히 사라진다 — 자원 6행이 통째로 빈 화면이 되고
 * 에러는 없다. 각 파일을 따로 보는 테스트로는 그 어긋남을 잡을 수 없어 한 자리에서 본다.
 *
 * <p>메서드 이름을 바꾸는 리팩터링은 컴파일러가 잡아 주지 않는다 — 한쪽은 애노테이션 문자열이다.
 */
class MeasuredPoolContractTest {

    private static final String MAIN_POOL = "mainDataSource";

    @Test
    @DisplayName("자원 공급자가 지목하는 이름과 운영 풀 빈 이름이 같다")
    void theMeasuredPoolNameMatchesTheDeclaredBeanName() {
        assertThat(qualifierOfDataSourceParameter())
                .as("자원 공급자가 지목한 풀 이름")
                .isEqualTo(mainPoolBeanName());
    }

    /**
     * 조건과 주입이 같은 것을 보는지 확인한다. 조건을 타입으로 두면 스위치를 끈 모듈에서 Boot
     * 기본 {@code dataSource} 가 조건만 통과시키고 이름 주입이 실패해 컨텍스트가 죽는다.
     */
    @Test
    @DisplayName("등록 조건도 주입과 같은 이름을 본다")
    void theRegistrationConditionGuardsTheSameName() {
        ConditionalOnBean condition = resourceProviderMethod().getAnnotation(ConditionalOnBean.class);

        assertThat(condition).as("조건이 없으면 이름 주입 실패가 기동 실패가 된다").isNotNull();
        assertThat(condition.name()).contains(MAIN_POOL);
    }

    /** 운영 풀 빈 이름은 메서드 이름에서 나온다. 메서드를 바꾸면 여기서 걸린다. */
    private static String mainPoolBeanName() {
        return Arrays.stream(MainDataSourceConfig.class.getDeclaredMethods())
                .map(Method::getName)
                .filter(MAIN_POOL::equals)
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "MainDataSourceConfig 에 " + MAIN_POOL + " 빈 메서드가 없다"));
    }

    private static String qualifierOfDataSourceParameter() {
        for (Parameter parameter : resourceProviderMethod().getParameters()) {
            if (parameter.getType() == DataSource.class) {
                Qualifier qualifier = parameter.getAnnotation(Qualifier.class);
                assertThat(qualifier)
                        .as("한정자가 없으면 @Primary 가 옮겨갈 때 측정 대상이 조용히 바뀐다")
                        .isNotNull();
                return qualifier.value();
            }
        }
        throw new AssertionError("resourceProvider 가 DataSource 를 받지 않는다");
    }

    private static Method resourceProviderMethod() {
        return Arrays.stream(ApiObservationAutoConfiguration.class.getDeclaredMethods())
                .filter(method -> method.getReturnType() == ResourceProvider.class)
                .findFirst()
                .orElseThrow(() -> new AssertionError("resourceProvider 빈 메서드가 없다"));
    }
}
