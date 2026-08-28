package com.kafkick.infra.mq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Kafka 접속 정보. {@code kafka.yml} 이 소유한다.
 *
 * <p><b>Boot 의 Kafka 자동설정을 쓰지 않는다.</b> 이 모듈은 {@code spring-kafka} 만 의존하고
 * {@code spring-boot-kafka} 를 의존하지 않아 {@code spring.kafka.*} 바인딩도 자동 빈도 없다.
 * 일부러 그렇게 뒀다 — 자동설정은 {@code KafkaTemplate} 하나와 리스너 컨테이너 팩토리 하나를
 * 만드는데, 이 티켓의 전제가 <b>프로듀서 둘을 완전히 분리</b>하고
 * {@code auto.offset.reset} 을 토픽마다 반대로 두는 것이다. 공용 빈이 하나라도 남아 있으면
 * 나중에 누군가 그걸 주입해 쓰고, 그 순간 분리가 무효가 된다.
 *
 * <h2>여기서 값을 검증하지 않는다</h2>
 *
 * {@code @ConfigurationPropertiesScan} 은 {@code kafka.enabled} 와 무관하게 이 레코드를
 * 등록한다. 그래서 {@code @NotEmpty} 를 걸면 <b>스위치를 끈 회차에서도 검증이 돌아</b>,
 * Kafka 를 쓰지도 않는 v1·v2 배포가 빈 {@code KAFKA_BOOTSTRAP_SERVERS} 하나로 기동에
 * 실패한다. 검증은 프로듀서·어드민이 실제로 만들어지는 자리
 * ({@code KafkaProducerSupport.requireBootstrapServers})에서 한다 — 거기는
 * {@code @ConditionalOnProperty} 안쪽이라 켰을 때만 돈다.
 *
 * <p>스위치 값({@code kafka.enabled})을 필드로 두지 않는 것도 같은 이유다. 조건은 전부
 * {@code @ConditionalOnProperty} 가 문자열로 평가하므로, 필드로도 들고 있으면 같은 설정에
 * 소유자가 둘이 되고 둘이 어긋나도 아무도 모른다.
 */
@ConfigurationProperties("kafka")
public record KafkaConnectionProperties(String bootstrapServers) {
}
