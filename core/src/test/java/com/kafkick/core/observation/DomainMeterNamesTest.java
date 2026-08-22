package com.kafkick.core.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.consistency.ConsistencyGapType;

/**
 * 미터 이름과 상태 코드는 등록하는 쪽(batch · infra:mq)과 읽는 쪽(조회 API)이 함께 쓰는
 * 공개 계약이다.
 * 바뀌면 예외 없이 화면만 비므로 골든값으로 고정한다.
 */
class DomainMeterNamesTest {

    @Test
    @DisplayName("값 미터와 상태 미터는 이름이 짝을 이룬다")
    void valueAndStateNamesArePaired() {
        assertThat(DomainMeterNames.CONSISTENCY_GAP).isEqualTo("app.consistency.gap");
        assertThat(DomainMeterNames.CONSISTENCY_GAP_STATE)
            .isEqualTo(DomainMeterNames.CONSISTENCY_GAP + ".state");
        assertThat(DomainMeterNames.OVER_ISSUED_STATE)
            .isEqualTo(DomainMeterNames.OVER_ISSUED + ".state");
        assertThat(DomainMeterNames.QUEUE_LENGTH_STATE)
            .isEqualTo(DomainMeterNames.QUEUE_LENGTH + ".state");
        assertThat(DomainMeterNames.STOCK_REMAINING_STATE)
            .isEqualTo(DomainMeterNames.STOCK_REMAINING + ".state");
        assertThat(DomainMeterNames.LAST_SUCCESSFUL_ISSUE_EPOCH_STATE)
            .isEqualTo(DomainMeterNames.LAST_SUCCESSFUL_ISSUE_EPOCH + ".state");
        assertThat(DomainMeterNames.CONSISTENCY_SEVERITY_STATE)
            .isEqualTo(DomainMeterNames.CONSISTENCY_SEVERITY + ".state");
        assertThat(DomainMeterNames.CONSISTENCY_SOURCE_SKEW_SECONDS_STATE)
            .isEqualTo(DomainMeterNames.CONSISTENCY_SOURCE_SKEW_SECONDS + ".state");

        // OBS-17. 운영 지침(kafka.yml.example)이 노출 이름을 손으로 적고 있어 계약의 무게가 같다.
        assertThat(DomainMeterNames.KAFKA_ATTEMPT_PUBLISH_FAILURES)
            .isEqualTo("app.kafka.attempt.publish.failures");
        assertThat(DomainMeterNames.KAFKA_TOPICS_PROVISIONED)
            .isEqualTo("app.kafka.topics.provisioned");
        assertThat(DomainMeterNames.KAFKA_TOPICS_PROVISIONED_STATE)
            .isEqualTo(DomainMeterNames.KAFKA_TOPICS_PROVISIONED + ".state");
        assertThat(DomainMeterNames.KAFKA_TOPICS_PROVISIONED_CAUSE)
            .isEqualTo(DomainMeterNames.KAFKA_TOPICS_PROVISIONED + ".cause");
        assertThat(DomainMeterNames.TAG_CAUSE).isEqualTo("cause");
    }

    @Test
    @DisplayName("gap 라벨 값은 enum 상수명 리팩터링에 흔들리지 않는다")
    void gapTagValuesAreFixed() {
        assertThat(Arrays.stream(ConsistencyGapType.values()).map(DomainMeterNames::gapTagValue))
            .containsExactly("active_db", "lua", "persist", "db_counter");
    }

    @Test
    @DisplayName("상태 코드는 enum 선언 순서와 무관하게 고정이다")
    void statusCodesAreStable() {
        assertThat(SourceStatusCode.of(SourceStatus.VALID)).isEqualTo(1);
        assertThat(SourceStatusCode.of(SourceStatus.PENDING)).isEqualTo(2);
        assertThat(SourceStatusCode.of(SourceStatus.WARMING_UP)).isEqualTo(3);
        assertThat(SourceStatusCode.of(SourceStatus.STALE)).isEqualTo(4);
        assertThat(SourceStatusCode.of(SourceStatus.NO_TRAFFIC)).isEqualTo(5);
        assertThat(SourceStatusCode.of(SourceStatus.UNAVAILABLE)).isEqualTo(6);
        assertThat(SourceStatusCode.of(SourceStatus.N_A)).isEqualTo(7);
    }
}
