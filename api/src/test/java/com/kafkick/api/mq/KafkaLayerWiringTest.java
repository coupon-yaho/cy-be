package com.kafkick.api.mq;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;

import com.kafkick.ApiApplication;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * OBS-17 의 설정 계층이 api 컨텍스트에 실제로 실리는지 본다. infra:mq 는 {@code runtimeOnly}
 * 의존이라 <b>본 코드가 이 타입들을 컴파일 타임에 못 본다</b> — 스캔이 끊겨도 컴파일은 통과하고
 * 앱은 정상 기동한다. 그 상태에서 관측 이벤트만 조용히 사라진다.
 *
 * <p>그래서 타입이 아니라 <b>빈 이름</b>으로 확인한다. 여기서 타입으로 받으면 runtimeOnly
 * 경계가 무너져 이 테스트가 지키려던 성질 자체가 사라진다.
 *
 * <p>부팅 클래스를 명시하는 이유 — 이 패키지에서 위로 올라가면 다른 테스트의 최소 앱
 * ({@code ManagementExposureTest.TestApp})이 먼저 잡힌다. 그 앱에는 스캔 대상이 없어서
 * 이 테스트가 검사하려던 것이 통째로 사라진다.
 */
@SpringBootTest(classes = ApiApplication.class,
        properties = { "kafka.enabled=true", "kafka.provision-topics=false" })
@Import(MySqlContainerConfig.class)
class KafkaLayerWiringTest {

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("프로듀서 두 벌과 발행기가 api 컨텍스트에 올라온다")
    void kafkaConfigLayerIsScannedFromTheApiContext() {
        assertThat(context.getBeanNamesForType(EventRecorder.class))
                .as("빈이 있는 것과 발급 경로가 그걸 잡는 것은 다르다 — 자동설정의 NoOp 이 이기면"
                        + " 발급은 정상인데 화면의 attempt 만 영원히 0 이다")
                .containsExactly("attemptEventPublisher");

        assertThat(context.containsBean("attemptKafkaTemplate")).isTrue();
        assertThat(context.containsBean("persistKafkaTemplate")).isTrue();
        assertThat(context.containsBean("attemptEventPublisher"))
                .as("발행기가 없으면 max.block.ms=0 이 삼킬 사람 없는 예외가 된다")
                .isTrue();
        assertThat(context.getBeanDefinitionNames())
                .as("토픽 3 + DLT 2")
                .contains("issuePersistTopic", "issueAttemptTopic", "notifyTopic",
                        "issuePersistDltTopic", "notifyDltTopic");
    }
}
