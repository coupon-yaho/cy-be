package com.kafkick.infra.mq.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.Assert;

import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.databind.json.JsonMapper;

/**
 * 두 프로듀서가 함께 쓰는 조각들. <b>어느 한쪽에 두면 의존 방향이 뒤집힌다.</b>
 *
 * <p>처음에는 이 메서드들이 {@link AttemptProducerConfig} 에 있었고
 * {@link PersistProducerConfig} 와 {@link KafkaTopicConfig} 가 그걸 불렀다. 그러면
 * <b>정합성을 좌우하는 경로(persist)가 유실을 감수하는 경로(attempt)에 컴파일 타임으로
 * 묶인다.</b> 방향이 거꾸로다 — attempt 쪽을 "어차피 관측용" 이라며 손대는 순간 persist 가
 * 같이 흔들리고, 더 나쁘게는 시맨틱만 attempt 에 맞게 바뀌어 persist 가 조용히 다른 설정을
 * 받는다.
 *
 * <p>두 프로듀서를 팩토리부터 나눠 둔 이유가 등급이 정반대라서인데, 그 분리가 <b>설정을
 * 만드는 코드에서</b> 무너지면 의미가 없다.
 */
final class KafkaProducerSupport {

    private KafkaProducerSupport() {}

    /**
     * 접속 정보 검증은 <b>여기</b>가 자리다. 프로퍼티 레코드에 {@code @NotEmpty} 를 걸면
     * {@code @ConfigurationPropertiesScan} 이 스위치와 무관하게 검증을 돌려서,
     * {@code kafka.enabled=false} 인 회차가 <b>쓰지도 않는 Kafka 설정 때문에</b> 기동에 실패한다.
     * 이 메서드를 부르는 자리는 전부 {@code @ConditionalOnProperty} 안쪽이다.
     */
    static String requireBootstrapServers(KafkaConnectionProperties properties) {
        Assert.hasText(properties.bootstrapServers(),
                "kafka.bootstrap-servers 가 비어 있다. kafka.enabled 를 켰다면 접속 정보가 필요하다.");
        return properties.bootstrapServers();
    }

    /**
     * 타입 헤더를 끈다. 켜면 발신 모듈의 <b>클래스 이름</b>이 헤더로 나가서, 클래스를 옮기거나
     * 이름을 바꾸는 순간 이미 브로커에 쌓인 레코드가 컨슈머에서 안 풀린다.
     *
     * <p>매퍼는 애플리케이션 공용 {@code JsonMapper} 를 쓴다. 여기서 새로 만들면 core 의 골든
     * 픽스처가 검증한 형식과 조용히 달라진다.
     */
    static JacksonJsonSerializer<Object> valueSerializer(JsonMapper jsonMapper) {
        JacksonJsonSerializer<Object> serializer = new JacksonJsonSerializer<>(jsonMapper);
        serializer.setAddTypeInfo(false);
        return serializer;
    }

    /**
     * 클라이언트 자체 메트릭({@code buffer-available-bytes} · {@code record-error-rate} 등)을
     * 레지스트리에 붙인다. 이게 없으면 실패 카운터가 튀었을 때 <b>버퍼 만석인지 브로커 다운인지
     * 구분할 데이터가 없다.</b>
     *
     * <p>레지스트리가 없으면 그냥 건너뛴다 — 관측 설비의 부재가 발급 API 기동 실패가 되면 안 된다.
     */
    static void bindClientMetrics(
            DefaultKafkaProducerFactory<String, Object> factory,
            ObjectProvider<MeterRegistry> meterRegistries
    ) {
        meterRegistries.ifAvailable(registry -> factory.addListener(new MicrometerProducerListener<>(registry)));
    }
}
