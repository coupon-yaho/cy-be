package com.kafkick;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 관측-Batch 포트 계약은 batch 가 내보낼 것을 <b>4종</b>으로 적는다 — 정합성 gap · 대기열 길이 ·
 * 재고 잔량 · Kafka persist lag. 이 PR(OBS-5)은 앞의 3종만 낸다.
 *
 * <p>Kafka lag 은 AdminClient 로 committed offset 과 end offset 을 비교해야 하는데, 그 배선은
 * OBS-17(Kafka 설정 계층) 소유다. 여기서 통로부터 새로 여는 것은 그 티켓과 충돌한다.
 *
 * <p>이 테스트가 깨지면 OBS-17 이 붙었다는 뜻이다. 그때 할 일 —
 * <ul>
 *   <li>Kafka lag Gauge 를 batch/observation 에 추가하고 이름을 {@code DomainMeterNames} 에 둔다.
 *   <li>값·상태 분리 규약을 그대로 적용한다 — lag 을 못 읽으면 0 이 아니라 UNAVAILABLE 이다.
 *   <li>수집 경로가 하나 늘므로 {@code app.observation.collect.last.success.epoch} 에
 *       {@code path="kafka"} 를 추가한다.
 * </ul>
 */
class BatchKafkaLagAssumptionTest {

    @Test
    @DisplayName("batch 에 아직 Kafka 통로가 없다 — Kafka lag 을 OBS-17 이후로 미룬 전제다")
    void kafkaChannelDoesNotExistYet() throws Exception {
        Path buildFile = Path.of("build.gradle");

        assertThat(buildFile).as("모듈 빌드 파일을 못 읽으면 이 감시는 무효다").isRegularFile();
        assertThat(Files.readString(buildFile, StandardCharsets.UTF_8))
                .as("Kafka 의존성이 들어왔다. Kafka persist lag Gauge 를 추가한다"
                        + " — 클래스 주석의 세 항목을 보라")
                .doesNotContain("spring-kafka");
    }
}
