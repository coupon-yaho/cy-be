package com.kafkick.infra.mq.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.kafkick.infra.mq.attempt.AttemptFailureCounter;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import tools.jackson.databind.json.JsonMapper;

/**
 * 관측 로그({@code coupon.issue.attempt}) 전용 프로듀서. <b>발급 경로에서 이 프로듀서를 탄다.</b>
 *
 * <p>이 티켓에서 가장 위험한 실수는 프로듀서를 하나로 묶는 것이다. 하나의
 * {@code KafkaTemplate} 을 persist 와 공유하면 로그 전송 실패가 발급 예외로 올라온다 —
 * 관측이 서비스를 죽이는 최악의 형태다. 그래서 {@link PersistProducerConfig} 와
 * <b>ProducerFactory 부터</b> 나눠 둔다. 설정 맵만 다르게 주는 것으로는 부족하다. 팩토리가
 * 하나면 버퍼도 하나라, 느린 persist 가 버퍼를 채우면 attempt 발행이 같이 막힌다.
 *
 * <h2>{@code max.block.ms} 를 50ms 로 둔다</h2>
 *
 * 기본값은 60초다. 그대로 두면 버퍼가 찼거나 메타데이터가 아직 없을 때 {@code send()} 가
 * <b>호출 스레드에서</b> 최대 60초를 기다린다. 그 스레드는 발급 요청을 처리하던 톰캣
 * 워커다 — 부하 테스트 중에 정확히 그 상황이 온다.
 *
 * <p><b>0 이 아니라 50 인 이유</b> — 티켓 본문은 0 을 적었지만, 0 이면 메타데이터를 기다릴
 * 예산까지 0 이 되어 <b>재기동 직후 첫 이벤트들이 통째로 버려진다.</b> 브로커가 멀쩡해도
 * 그렇다. 부하 시험의 램프업 구간이 정확히 그 구간이라 관측이 가장 필요한 순간에 화면이
 * 빈다. 50ms 는 기본값의 1/1200 이라 "발급 경로를 막지 않는다" 는 성질을 유지하면서 그
 * 구멍만 닫는다.
 *
 * <p>{@code acks=0} 이라 브로커 응답을 기다리지 않고, {@code enable.idempotence} 는 끈다.
 * 멱등 프로듀서는 {@code acks=all} 을 요구해서 켜는 순간 acks 설정과 충돌한다. 이 토픽은
 * 유실을 감수하는 대신 발급 경로를 절대 막지 않는 쪽을 택한 것이고, 그래서 TPS·성공률·정합성의
 * 원천으로 쓰지 않는다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("kafka.enabled")
@EnableConfigurationProperties(KafkaConnectionProperties.class)
public class AttemptProducerConfig {

    private static final Logger log = LoggerFactory.getLogger(AttemptProducerConfig.class);

    /** 메타데이터 미확보·버퍼 만석일 때 호출 스레드가 붙잡히는 상한. 기본값은 60초다. */
    static final int MAX_BLOCK_MS = 50;

    /**
     * 브로커가 죽은 것을 <b>2초 안에</b> 지표로 안다. 클라이언트가
     * {@code delivery >= linger + request} 를 검증하므로 request 도 함께 내린다.
     */
    static final int REQUEST_TIMEOUT_MS = 1_000;
    static final int DELIVERY_TIMEOUT_MS = 2_000;

    /**
     * 기본값은 32MiB 다. 유실을 허용하는 스트림이 발급 API 힙을 그만큼 물고 있을 이유가 없다 —
     * 버퍼가 차면 {@code max.block.ms=50} 이 걸려 50ms 뒤 실패로 떨어지고, 그게 의도한 동작이다.
     */
    static final long BUFFER_MEMORY = 8L * 1024 * 1024;

    /** 메트릭·JMX 에서 어느 프로듀서인지 구분하는 이름. 안 주면 {@code producer-1} 처럼 생성 순서로 붙는다. */
    static final String CLIENT_ID = "coupon-attempt";

    @Bean
    public ProducerFactory<String, Object> attemptProducerFactory(
            KafkaConnectionProperties properties,
            JsonMapper jsonMapper,
            ObjectProvider<MeterRegistry> meterRegistries
    ) {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KafkaProducerSupport.requireBootstrapServers(properties));
        config.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        config.put(ProducerConfig.ACKS_CONFIG, "0");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        // 버퍼가 만석이어도 발급 경로를 50ms 이상 붙잡지 않는다.
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS);
        // 멱등성이 꺼진 채로 재시도하면 같은 파티션 안에서 순서가 뒤집힌다. acks=0 이라
        // 재시도 경로가 거의 열리지 않지만, 열릴 때 무엇이 일어나는지를 설정으로 못박는다.
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        // max.block.ms 는 '보내기 전' 만 막는다. 이미 accumulator 에 앉은 레코드의 실패는
        // delivery.timeout.ms 가 만료돼야 알 수 있어서, 기본값 120초면 그동안 지표가 0 이다.
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, DELIVERY_TIMEOUT_MS);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, BUFFER_MEMORY);

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(
                config, new StringSerializer(), KafkaProducerSupport.valueSerializer(jsonMapper));
        KafkaProducerSupport.bindClientMetrics(factory, meterRegistries);
        return factory;
    }

    /**
     * 실패 카운터. <b>템플릿에 {@code ProducerListener} 로 붙이지 않는다</b> —
     * {@code AttemptEventPublisher} 의 {@code whenComplete} 와 둘 다 세면 발행 1회가 2건으로
     * 잡힌다(직접 돌려서 확인했다). 세는 자리는 하나뿐이어야 한다.
     *
     * <p>레지스트리가 없으면 앱을 세우는 대신 빈 레지스트리로 떨어진다. 다만 <b>조용히</b>
     * 넘어가지는 않는다 — 그 상태에서는 실패 카운터가 어떤 scrape 에도 안 나와서,
     * "관측이 죽었다" 는 사실 자체가 관측되지 않는다.
     */
    @Bean
    public AttemptFailureCounter attemptFailureCounter(ObjectProvider<MeterRegistry> meterRegistries) {
        return new AttemptFailureCounter(meterRegistries.getIfAvailable(() -> {
            log.warn("MeterRegistry 가 없다 — attempt 발행 실패 카운터가 노출되지 않는다");
            return new SimpleMeterRegistry();
        }));
    }

    /**
     * <b>기본 리스너를 떼어 낸다.</b> {@code KafkaTemplate} 은 아무것도 지정하지 않으면
     * {@code LoggingProducerListener} 를 달고 나오는데, 그것이 실패 <b>건당</b> ERROR 를
     * 찍는다 — 키와 페이로드 앞 100자를 함께 싣는다(내 테스트 산출물에서 실제로 확인했다:
     * {@code key='7' payload='IssuanceFlowEvent[...'}).
     *
     * <p>그 로그는 콜백에서 나므로 <b>호출 스레드</b>, 즉 톰캣 워커가 찍는다. 이 저장소에는
     * 비동기 appender 설정이 없어 동기 ConsoleAppender 다. 브로커가 죽은 채로 초당 수천 건이
     * 들어오면 그 포맷·write 비용을 발급 요청이 전부 문다 — {@code max.block.ms} 를 60초에서
     * 50ms 로 낮춰 번 것을 로그 I/O 가 그대로 되돌린다.
     *
     * <p>attempt 의 실패는 {@link AttemptFailureCounter} 가 카운터와 10초 요약 로그로 이미
     * 다룬다. 여기서 한 번 더 찍을 이유가 없다.
     */
    @Bean
    public KafkaTemplate<String, Object> attemptKafkaTemplate(
            @Qualifier("attemptProducerFactory") ProducerFactory<String, Object> attemptProducerFactory
    ) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(attemptProducerFactory);
        template.setProducerListener(null);
        return template;
    }

}
