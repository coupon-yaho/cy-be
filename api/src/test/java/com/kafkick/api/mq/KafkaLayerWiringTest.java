package com.kafkick.api.mq;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import com.kafkick.ApiApplication;
import com.kafkick.api.observation.issuance.CompositeEventRecorder;
import com.kafkick.api.observation.issuance.MeterEventRecorder;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.testsupport.CommittedConfigStager;

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
@SpringBootTest(classes = ApiApplication.class, properties = {
        "spring.config.location=file:build/cy266-kafka-wiring/kafka.yml",
        "kafka.enabled=true",
        "kafka.provision-topics=false",
        "observation.datasource.enabled=false",
        // [OBS-36] 관리자 화면 fixture 는 이제 기본값이 꺼짐이다. 이 테스트는 Kafka 배선을
        // 보는 것이지 fixture 를 보는 것이 아니라서, 컨텍스트가 뜨도록 켜 준다.
        "admin.mock.enabled=true",
        "coupon.idempotency.wait-timeout=1s",
        "coupon.idempotency.poll-interval=50ms",
        "coupon.idempotency.stale-after=30s",
        "coupon.round-generation.schedule-zone=Asia/Seoul",
        "coupon.round-generation.max-days=30",
        "benchmark.topology.tomcat-workers-total=60",
        "benchmark.topology.hikari-pool-total=12",
        "benchmark.topology.mysql-max-connections=50"
})
@Import(MySqlContainerConfig.class)
class KafkaLayerWiringTest {

    private static final Path STAGED_CONFIG = Path.of("build/cy266-kafka-wiring/kafka.yml");

    /**
     * <b>커밋된 {@code .example} 을 깔고 그것만 읽는다.</b> 실행용 {@code *.yml} 은 전부
     * gitignore 라, 개발자가 손으로 복사해 둔 로컬 파일에 기대면 갓 클론한 환경에서는 이 테스트가
     * {@code kafka.bootstrap-servers} 를 못 찾아 컨텍스트 기동에서 죽는다(실측 — 8개를 치우고
     * 돌리니 {@code Assert} 에서 멈췄다). 이 저장소의 다른 api 테스트들과 같은 방식이다.
     */
    @BeforeAll
    static void stageKafkaConfig() throws Exception {
        CommittedConfigStager.stage(STAGED_CONFIG, "kafka.yml.example");
    }

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("Kafka 발행기와 캠페인 미터 합성기가 함께 api 컨텍스트에 올라온다")
    void kafkaConfigLayerIsScannedFromTheApiContext() {
        assertThat(context.getBeanNamesForType(EventRecorder.class))
                .as("발급 이벤트는 Kafka 발행과 JVM 내 미터를 함께 지나야 한다")
                .contains("attemptEventPublisher", "meterEventRecorder", "issuanceEventRecorder");
        assertThat(context.getBean(EventRecorder.class)).isInstanceOf(CompositeEventRecorder.class);
        assertThat(context.getBeansOfType(MeterEventRecorder.class)).hasSize(1);
        assertThat(context.containsBean("fallbackEventRecorder")).isFalse();

        assertThat(context.containsBean("attemptKafkaTemplate")).isTrue();
        assertThat(context.containsBean("persistKafkaTemplate")).isTrue();
        assertThat(context.containsBean("attemptEventPublisher"))
                .as("발행기가 없으면 max.block.ms=0 이 삼킬 사람 없는 예외가 된다")
                .isTrue();
        assertThat(context.getBeanDefinitionNames())
                .as("토픽 3 + DLT 2")
                .contains("issuePersistTopic", "issueAttemptTopic", "notifyTopic",
                        "issuePersistDltTopic", "notifyDltTopic");
        assertThat(context.getBeanNamesForType(
            com.kafkick.api.admin.benchmark.BenchmarkStartOrchestrator.class)).isEmpty();
        assertThat(context.getBeanNamesForType(
            com.kafkick.api.admin.benchmark.BenchmarkFinalizeOrchestrator.class)).isEmpty();
    }
}
