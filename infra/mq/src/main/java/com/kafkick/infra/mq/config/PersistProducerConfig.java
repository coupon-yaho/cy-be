package com.kafkick.infra.mq.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.LoggingProducerListener;

import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.databind.json.JsonMapper;

/**
 * 발급 영속화({@code coupon.issue.persist})와 발송({@code coupon.notify}) 프로듀서.
 * v3 의 비동기 영속화가 <b>여기를 타게 될</b> 자리다 — 이 티켓은 설정 계층까지고,
 * {@code send()} 를 부르는 발행 지점은 아직 없다(OBS-15 · A-04).
 *
 * <p>attempt 와 정반대의 등급이다. 여기서 한 건이 유실되면 <b>영구 미영속 발급</b>이 되고,
 * 그건 이 프로젝트의 정합성 위반 판정 대상이다. 그래서 {@code acks=all} 과 멱등 프로듀서를
 * 함께 켠다 — 멱등성이 없으면 재시도가 중복 발행이 되어 유실을 막는 대가로 중복을 만든다.
 *
 * <h2>상한을 셋 다 명시한다 — 기본값은 최악 180초다</h2>
 *
 * 기본값 조합은 {@code max.block 60s} + {@code delivery 120s} 다. 호출부가 결과를 기다리면
 * (v3 에서 재고 락 안에 {@code send().get()} 이 들어가면) <b>그 락이 최대 180초 잡힌다.</b>
 * 재시도 횟수는 상한이 아니다 — {@code retries} 기본값이 사실상 무한이라 실질 상한은
 * {@code delivery.timeout.ms} 하나뿐이다.
 *
 * <pre>
 * max.block 3s  → 버퍼·메타데이터 대기
 * request   5s  → 요청 한 번의 응답 대기
 * delivery  15s → 재시도를 전부 포함한 총 상한   (최악 180s → 18s)
 * </pre>
 *
 * <p>attempt 와 달리 {@code max.block.ms} 를 0 근처로 낮추지 않는다. 이쪽은 <b>기다리는 게
 * 맞다</b> — 버퍼가 찼다고 그냥 던져 버리면 그 발급 건이 사라진다. 다만 60초는 아니다.
 *
 * <p>circuit breaker 는 이 프로듀서<b>에만</b> 건다(OBS-15 · A-04). attempt 는
 * fire-and-forget 이라 실패를 삼키고, 삼킨 실패는 차단기가 셀 수 없다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("kafka.enabled")
@EnableConfigurationProperties(KafkaConnectionProperties.class)
public class PersistProducerConfig {

    /**
     * 클라이언트가 기동에서 거부하는 조건이 {@code delivery >= linger + request} 라 셋을 한
     * 가족으로 본다. 기본값(Kafka 4 에서 5ms)에 맡기면 그 값이 버전에 따라 바뀌었을 때
     * 우리 delivery 가 조용히 부적격이 된다.
     */
    static final int LINGER_MS = 5;

    static final int MAX_BLOCK_MS = 3_000;
    static final int REQUEST_TIMEOUT_MS = 5_000;
    static final int DELIVERY_TIMEOUT_MS = 15_000;

    static final String CLIENT_ID = "coupon-persist";

    @Bean
    public ProducerFactory<String, Object> persistProducerFactory(
            KafkaConnectionProperties properties,
            JsonMapper jsonMapper,
            ObjectProvider<MeterRegistry> meterRegistries
    ) {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KafkaProducerSupport.requireBootstrapServers(properties));
        config.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, DELIVERY_TIMEOUT_MS);

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(
                config, new StringSerializer(), KafkaProducerSupport.valueSerializer(jsonMapper));
        KafkaProducerSupport.bindClientMetrics(factory, meterRegistries);
        return factory;
    }

    /**
     * attempt 와 달리 기본 로거를 <b>남긴다</b>. 이쪽은 아직 발행 지점도 실패 카운터도 없어서
     * (둘 다 OBS-15 · A-04 몫이다) 실패를 알릴 수단이 로그뿐이고, persist 한 건의 유실은
     * 정합성 위반 후보다.
     *
     * <p>다만 <b>키와 페이로드는 찍지 않는다.</b> 기본값은 실패마다 키(=issuanceCode)와 본문
     * 앞 100자를 로그로 흘리는데, 여기에는 회원·발급 식별자가 들어 있다.
     */
    @Bean
    public KafkaTemplate<String, Object> persistKafkaTemplate(
            @Qualifier("persistProducerFactory") ProducerFactory<String, Object> persistProducerFactory
    ) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(persistProducerFactory);
        template.setProducerListener(quietListener());
        return template;
    }

    /**
     * DLT 발행은 <b>이 템플릿으로만</b> 한다.
     *
     * <p>{@code KafkaTemplate} 빈이 둘이라 타입으로 주입하면 어느 쪽이 올지 모른다. attempt 쪽을
     * 집으면 {@code acks=0}·{@code retries=0} 이라 <b>격리본이 브로커 확인 없이 날아가고</b>,
     * 실패해도 리스너가 없어 카운터도 안 오른다. DLT 의 목적은 잃지 않는 것인데 유실을 감수하는
     * 프로듀서에 얹히는 셈이다.
     *
     * <p>persist 팩토리를 재사용해 {@code acks=all}·멱등성을 그대로 물려받는다.
     */
    @Bean
    public KafkaTemplate<String, Object> deadLetterKafkaTemplate(
            @Qualifier("persistProducerFactory") ProducerFactory<String, Object> persistProducerFactory
    ) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(persistProducerFactory);
        // 팩토리만 물려받고 템플릿 정책을 안 물려받으면, persist 에서 가린 페이로드가 여기서 부활한다.
        // DLT 로 가는 레코드는 원본 그대로다.
        template.setProducerListener(quietListener());
        return template;
    }

    /**
     * 실패를 알리되 <b>내용은 찍지 않는</b> 로거. 기본값은 실패마다 키와 본문 앞 100자를 흘리는데,
     * 거기에는 회원·발급 식별자가 들어 있다.
     */
    private static LoggingProducerListener<String, Object> quietListener() {
        LoggingProducerListener<String, Object> listener = new LoggingProducerListener<>();
        listener.setIncludeContents(false);
        return listener;
    }
}
