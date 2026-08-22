package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;
import com.kafkick.infra.mq.attempt.AttemptFailureCounter;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * 미터 이름이 <b>실제 scrape 출력</b>에서 어떻게 보이는지 대조한다.
 *
 * <p>Micrometer 이름과 Prometheus 노출 이름은 같지 않다 — 점이 밑줄이 되고 Counter 에는
 * {@code _total} 이 붙는다. 조회하는 쪽(PromQL · 대시보드 · 운영 지침)은 <b>노출 이름</b>을
 * 쓰므로, 그 변환을 아무도 확인하지 않으면 이름이 어긋나도 예외 없이 값만 빈다.
 *
 * <p>이 저장소는 같은 사고를 이미 겪어 api 쪽에 전용 계약 테스트를 두었다. Kafka 미터도
 * 그 문을 지나게 한다 — 다만 여기서는 웹 컨텍스트 없이 레지스트리만 띄운다.
 */
class KafkaMeterScrapeContractTest {

    private final PrometheusMeterRegistry registry =
            new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    /**
     * <b>필드로 붙잡아 둔다.</b> Micrometer 의 게이지는 상태 객체를 {@code WeakReference} 로 든다
     * ({@code DefaultGauge.ref}). 지역 변수로 두면 GC 한 번에 값이 NaN 이 되어, NaN 을 기대하는
     * 단언은 공허하게 통과하고 값을 기대하는 단언은 간헐적으로 깨진다.
     *
     * <p>운영 경로는 안전하다 — 바인더가 싱글턴 빈이라 {@code ObjectProvider} 가 강참조로 산다.
     */
    private ObjectProvider<KafkaTopicProvisioner> provisioners;

    private KafkaTopicProvisioner provisioner;

    /** 이 테스트들은 {@code provisionOnce()} 를 직접 부르므로 실행기를 쓰지 않는다. 남기지도 않는다. */
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    void shutdownExecutor() {
        executor.shutdownNow();
    }

    /**
     * 상태 코드({@code UNAVAILABLE})만으로는 <b>재기동으로 낫는 실패</b>와 <b>낫지 않는 실패</b>가
     * 같은 6 으로 보인다. 이 스택에는 로그를 모아 보는 도구가 없어서 "로그로 가르라"는 지침이
     * 화면에서는 닿지 않는다 — 그래서 원인을 지표로 낸다.
     *
     * <p>꼬리표 값은 등록 시점에 고정되므로 닫힌 4값을 각각 한 시계열로 내고 현재 원인만 1 이다.
     */
    @Test
    @DisplayName("확인 실패의 원인이 닫힌 4개 시계열로 나온다")
    void theCauseOfTheFailureIsScrapable() {
        provisioner = new KafkaTopicProvisioner(
                () -> ProvisionOutcome.MISMATCHED, executor, 1, Duration.ofMillis(1));
        provisioners = presentProvider(provisioner);
        new KafkaTopicConfig().kafkaTopicProvisioningMeters(provisioners).bindTo(registry);

        provisioner.provisionOnce();
        String scrape = registry.scrape();

        assertThat(scrape)
                .as("원인을 못 가르면 재기동으로 낫지 않는 실패에 재기동을 반복한다")
                .contains("app_kafka_topics_provisioned_cause{cause=\"mismatched\"} 1.0")
                .contains("app_kafka_topics_provisioned_cause{cause=\"unconfirmed\"} 0.0")
                .contains("app_kafka_topics_provisioned_cause{cause=\"none\"} 0.0");
    }

    @Test
    @DisplayName("attempt 실패 카운터가 _total 접미사를 달고 나온다 — 실패가 0건이어도")
    void failureCounterAppearsInScrapeEvenBeforeAnyFailure() {
        new AttemptFailureCounter(registry);

        assertThat(registry.scrape())
                .as("실패 전에도 0 으로 보여야 '시계열 없음' 과 '아직 0' 이 구분된다")
                .contains(promName(DomainMeterNames.KAFKA_ATTEMPT_PUBLISH_FAILURES) + "_total")
                .contains("reason=\"timeout\"")
                .contains("topic=\"" + KafkaTopicConfig.ISSUE_ATTEMPT + "\"");
    }

    @Test
    @DisplayName("프로비저닝 값·상태 미터가 짝으로 나온다 — 프로비저너가 없어도")
    void provisioningMetersAppearEvenWithoutAProvisioner() {
        provisioners = absent();
        new KafkaTopicConfig().kafkaTopicProvisioningMeters(provisioners).bindTo(registry);
        String scrape = registry.scrape();

        assertThat(scrape)
                .contains(promName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED))
                .contains(promName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED_STATE));
        assertThat(scrapeValue(scrape, promName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED)))
                .as("확인하지 않은 값에 0 을 실으면 '확인했는데 0' 으로 읽힌다")
                .isNaN();
    }

    @Test
    @DisplayName("프로비저닝을 끈 회차는 N_A 상태로 나온다 — 실패가 아니다")
    void provisioningDisabledIsReportedAsNotApplicable() {
        provisioners = absent();
        new KafkaTopicConfig().kafkaTopicProvisioningMeters(provisioners).bindTo(registry);

        assertThat(scrapeValue(registry.scrape(),
                promName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED_STATE)))
                .isEqualTo(SourceStatusCode.of(SourceStatus.N_A));
    }

    /**
     * 프로비저너가 <b>있는</b> 경로. 이 경로를 아무도 안 태우면 값 함수가 잘못 바뀌어도
     * (예: NaN 대신 0) scrape 계약이 초록불로 남는다.
     */
    @Test
    @DisplayName("확인 전에는 값이 없고 PENDING, 반영되면 1 과 VALID 로 함께 움직인다")
    void valueAndStateMoveTogetherOnceProvisioned() {
        provisioner = new KafkaTopicProvisioner(() -> ProvisionOutcome.PROVISIONED, executor, 1, Duration.ZERO);
        provisioners = presentProvider(provisioner);
        new KafkaTopicConfig().kafkaTopicProvisioningMeters(provisioners).bindTo(registry);

        String beforeAttempt = registry.scrape();
        assertThat(scrapeValue(beforeAttempt, promName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED)))
                .as("시도 전에는 값이 없다")
                .isNaN();
        assertThat(scrapeValue(beforeAttempt, promName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED_STATE)))
                .isEqualTo(SourceStatusCode.of(SourceStatus.PENDING));

        provisioner.provisionOnce();

        String afterAttempt = registry.scrape();
        assertThat(scrapeValue(afterAttempt, promName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED)))
                .isEqualTo(1.0);
        assertThat(scrapeValue(afterAttempt, promName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED_STATE)))
                .as("값과 상태가 같은 scrape 안에서 어긋나면 읽는 쪽이 이상 상황으로 해석한다")
                .isEqualTo(SourceStatusCode.of(SourceStatus.VALID));
    }

    /** 점을 밑줄로 바꾼다. Micrometer 의 Prometheus 이름 규칙이다. */
    private static String promName(String meterName) {
        return meterName.replace('.', '_');
    }

    private static double scrapeValue(String scrape, String exposedName) {
        return scrape.lines()
                // 접두사만 보면 app_kafka_topics_provisioned 가 ..._state · ..._cause 줄을 집는다.
                .filter(line -> line.startsWith(exposedName + " ") || line.startsWith(exposedName + "{"))
                .findFirst()
                .map(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
                .orElseThrow(() -> new AssertionError("scrape 에 " + exposedName + " 이 없다"));
    }

    /** 프로비저닝을 끈 회차 — 프로비저너 빈이 아예 없다. */
    private static ObjectProvider<KafkaTopicProvisioner> absent() {
        return new ObjectProvider<>() {
            @Override
            public KafkaTopicProvisioner getObject() {
                throw new UnsupportedOperationException();
            }

            @Override
            public KafkaTopicProvisioner getObject(Object... args) {
                throw new UnsupportedOperationException();
            }

            @Override
            public KafkaTopicProvisioner getIfAvailable() {
                return null;
            }

            @Override
            public KafkaTopicProvisioner getIfUnique() {
                return null;
            }
        };
    }

    private static ObjectProvider<KafkaTopicProvisioner> presentProvider(KafkaTopicProvisioner value) {
        return new ObjectProvider<>() {
            @Override
            public KafkaTopicProvisioner getObject() {
                return value;
            }

            @Override
            public KafkaTopicProvisioner getObject(Object... args) {
                return value;
            }

            @Override
            public KafkaTopicProvisioner getIfAvailable() {
                return value;
            }

            @Override
            public KafkaTopicProvisioner getIfUnique() {
                return value;
            }
        };
    }
}
