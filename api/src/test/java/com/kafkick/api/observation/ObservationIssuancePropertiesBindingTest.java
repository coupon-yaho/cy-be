package com.kafkick.api.observation;

import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import com.kafkick.api.observation.issuance.CouponRoundMeterProperties;

import static com.kafkick.api.observation.ConfigContractFixture.defaultOf;
import static com.kafkick.api.observation.ConfigContractFixture.loadYaml;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * 설정 키가 <b>실제로 바인딩되는지</b> 본다.
 *
 * <p>relaxed binding 은 {@code -} 만 지우고 대조하므로 {@code ...retry-after} 는
 * {@code retryAfterSeconds} 에 닿지 않는다. 닿지 않으면 Boot 는 예외도 로그도 내지 않고 record 의
 * {@code null} 분기가 기본값을 채운다 — 설정 파일에는 3 이라고 적혀 있는데 나가는 값은 1 인
 * 상태가 되고, 그 사실은 부하 중에 "고쳤는데 안 바뀐다" 로만 드러난다.
 *
 * <p>그래서 키 이름을 리터럴로 적지 않고 <b>record 컴포넌트에서 유도</b>한다. 필드를 고치면
 * 여기서 깨지고, yml 만 고쳐도 여기서 깨진다.
 */
class ObservationIssuancePropertiesBindingTest {

    private static final String PREFIX = "observation.issuance";
    /** 이 절 아래의 하위 그룹. 자기 {@code @ConfigurationProperties} 를 갖는다. */
    private static final String NESTED_GROUP = "coupon-round";

    /**
     * yml 에 적힌 키가 전부 도달하는지 본다 — 반대 방향이 아니다.
     *
     * <p>모든 컴포넌트가 yml 에 있어야 한다고 걸면 안 된다. {@code attemptFailureLogInterval}
     * 처럼 코드 기본값으로 두고 일부러 노출하지 않은 손잡이가 있고, 그건 결함이 아니다.
     * <b>결함은 그 반대다</b> — 파일에 적혀 있는데 아무 필드에도 안 닿는 키. 그건 운영자가
     * 값을 바꿔도 아무 일이 안 일어나는 죽은 손잡이이고, 예외도 로그도 없다.
     */
    @Test
    void everyKeyInTheCommittedYamlReachesAProperty() throws IOException {
        Map<String, Object> section = issuanceSection();

        assertThat(section.keySet())
                .filteredOn(key -> !NESTED_GROUP.equals(key))
                .isSubsetOf(componentNames().stream().map(
                        ObservationIssuancePropertiesBindingTest::kebab).toList());
    }

    /**
     * yml 에 적힌 키로 넣은 값이 진짜 도착하는지 본다.
     *
     * <p>기본값과 다른 값을 넣는 것이 요점이다. 기본값을 넣으면 바인딩이 끊겨도 같은 숫자가
     * 나와 초록이다.
     */
    @Test
    void yamlKeysActuallyBindToTheRetryAfterFields() throws IOException {
        Map<String, Object> section = issuanceSection();
        Map<String, Object> source = new HashMap<>();
        section.forEach((key, value) -> {
            if (!NESTED_GROUP.equals(key)) {
                source.put(PREFIX + "." + key, defaultOf(String.valueOf(value)));
            }
        });
        source.put(PREFIX + "." + kebab("replayPendingRetryAfterSeconds"), 7);
        source.put(PREFIX + "." + kebab("gateNotReadyRetryAfterSeconds"), 9);

        ObservationIssuanceProperties bound = new Binder(
                new MapConfigurationPropertySource(source))
                .bind(PREFIX, ObservationIssuanceProperties.class)
.get();

        assertThat(bound.replayPendingRetryAfterSeconds()).isEqualTo(7);
        assertThat(bound.gateNotReadyRetryAfterSeconds()).isEqualTo(9);
    }

    /** 값을 안 주면 기본값 1 이다. 위 테스트가 기본값과 같은 값으로 통과하지 못하게 고정한다. */
    @Test
    void omittedRetryAfterFallsBackToTheDefault() {
        ObservationIssuanceProperties bound = new Binder(
                new MapConfigurationPropertySource(Map.of(
                        PREFIX + ".producer-instance-id", "api-1")))
                .bind(PREFIX, ObservationIssuanceProperties.class)
.get();

        assertThat(bound.replayPendingRetryAfterSeconds())
                .isEqualTo(ObservationIssuanceProperties.DEFAULT_RETRY_AFTER_SECONDS);
        assertThat(bound.gateNotReadyRetryAfterSeconds())
                .isEqualTo(ObservationIssuanceProperties.DEFAULT_RETRY_AFTER_SECONDS);
    }

    @Test
    void couponRoundMeterKeysBindThroughTheRenamedPrefix() {
        String prefix = PREFIX + ".coupon-round";
        CouponRoundMeterProperties bound = new Binder(new MapConfigurationPropertySource(Map.of(
                prefix + ".max-active-coupon-rounds", 7,
                prefix + ".retire-grace-period", "2m",
                prefix + ".tombstone-retention", "3h",
                prefix + ".tombstone-max-entries", 11)))
                .bind(prefix, CouponRoundMeterProperties.class)
                .get();

        assertThat(bound.resolvedMaxActiveCouponRounds()).isEqualTo(7);
        assertThat(bound.resolvedRetireGracePeriod()).isEqualTo(Duration.ofMinutes(2));
        assertThat(bound.resolvedTombstoneRetention()).isEqualTo(Duration.ofHours(3));
        assertThat(bound.resolvedTombstoneMaxEntries()).isEqualTo(11);
    }

    private static List<String> componentNames() {
        return Arrays.stream(ObservationIssuanceProperties.class.getRecordComponents())
                .map(RecordComponent::getName)
                .toList();
    }

    /** {@code replayPendingRetryAfterSeconds} → {@code replay-pending-retry-after-seconds} */
    private static String kebab(String componentName) {
        return componentName.replaceAll("([a-z0-9])([A-Z])", "$1-$2").toLowerCase();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> issuanceSection() throws IOException {
        // 커밋되는 것은 .example 이다. 실행용 observation.yml 은 gitignore 대상이라 갓 클론한
        // 환경에는 없다.
        Map<String, Object> root = loadYaml(Path.of("src/main/resources/observation.yml.example"));
        Map<String, Object> observation = (Map<String, Object>) root.get("observation");
        return (Map<String, Object>) observation.get("issuance");
    }
}
