package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KafkaConsumerGroupsTest {

    @Test
    @DisplayName("persist 와 notify 는 earliest, attempt 는 latest — 방향이 반대다")
    void offsetResetIsInvertedBetweenDurableAndObservabilityTopics() {
        assertThat(KafkaConsumerGroups.offsetResetOf(KafkaConsumerGroups.PERSIST))
                .isEqualTo(KafkaConsumerGroups.EARLIEST);
        assertThat(KafkaConsumerGroups.offsetResetOf(KafkaConsumerGroups.NOTIFY_DISPATCH))
                .isEqualTo(KafkaConsumerGroups.EARLIEST);
        assertThat(KafkaConsumerGroups.offsetResetOf(KafkaConsumerGroups.ATTEMPT_ARCHIVE))
                .as("재기동 때 밀린 수십만 건을 따라잡으면 관측이 DB 를 때린다")
                .isEqualTo(KafkaConsumerGroups.LATEST);
        assertThat(KafkaConsumerGroups.offsetResetOf(KafkaConsumerGroups.ATTEMPT_LIVE))
                .isEqualTo(KafkaConsumerGroups.LATEST);
    }

    @Test
    @DisplayName("attempt 의 두 그룹은 서로 다른 group.id 다")
    void attemptConsumersDoNotShareAGroup() {
        assertThat(KafkaConsumerGroups.ATTEMPT_LIVE)
                .as("같은 그룹이면 파티션을 나눠 가져 화면과 적재가 각각 절반만 본다")
                .isNotEqualTo(KafkaConsumerGroups.ATTEMPT_ARCHIVE);
        assertThat(KafkaConsumerGroups.offsetResets()).hasSize(5);
    }

    @Test
    @DisplayName("컨슈머 설정이 그룹별 offset 정책을 자동으로 붙인다 — OBS-15 가 이 문을 통과해야 한다")
    void consumerConfigCarriesTheGroupPolicy() {
        Map<String, Object> persist = KafkaConsumerGroups.consumerConfig(
                KafkaConsumerGroups.PERSIST, "localhost:9094");
        Map<String, Object> archive = KafkaConsumerGroups.consumerConfig(
                KafkaConsumerGroups.ATTEMPT_ARCHIVE, "localhost:9094");

        assertThat(persist).containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        assertThat(archive).containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        assertThat(persist).containsEntry(ConsumerConfig.GROUP_ID_CONFIG, KafkaConsumerGroups.PERSIST);
        assertThat(persist)
                .as("기본값 true 면 토픽 이름 오타가 그 이름의 토픽을 만들어 버린다 — 조용히 빈 컨슈머가 된다")
                .containsEntry(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        assertThat(persist).containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    }

    /**
     * 재처리는 격리본을 <b>처음부터</b> 다시 읽는 것이 목적이다. latest 면 이미 쌓인 것을 통째로
     * 건너뛰어 재처리 배치가 아무것도 재처리하지 않는다.
     */
    @Test
    @DisplayName("DLT 재처리는 earliest 다")
    void deadLetterReprocessingStartsFromTheBeginning() {
        assertThat(KafkaConsumerGroups.offsetResetOf(KafkaConsumerGroups.DLT_REPROCESS))
                .isEqualTo(KafkaConsumerGroups.EARLIEST);
    }

    @Test
    @DisplayName("정책이 정해지지 않은 그룹은 기본값으로 흘려보내지 않는다")
    void unknownGroupIsRejectedInsteadOfDefaulted() {
        assertThatThrownBy(() -> KafkaConsumerGroups.offsetResetOf("coupon-persist-v2"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaConsumerGroups.consumerConfig("coupon-persist-v2", "localhost:9094"))
                .as("설정을 만드는 자리에서도 막아야 한다. 여기가 OBS-15 가 지나갈 문이다")
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * 프로듀서 쪽에는 이 문이 있다({@code KafkaProducerSupport.requireBootstrapServers}).
     * 컨슈머만 통과하면 OBS-15 의 리스너가 브로커를 못 찾은 채 조용히 돈다 — 적재가 0건인데
     * 앱은 UP 이고, 그 조합이 이 관제 계층이 가장 늦게 알아채는 모양이다.
     */
    @Test
    @DisplayName("접속 정보가 비면 컨슈머 설정을 만들지 않는다")
    void emptyBootstrapServersIsRejected() {
        assertThatThrownBy(() -> KafkaConsumerGroups.consumerConfig(KafkaConsumerGroups.PERSIST, ""))
                .as("빈 리스트는 ConsumerConfig 가 허용한다 — 여기서 안 막으면 아무도 안 막는다")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaConsumerGroups.consumerConfig(KafkaConsumerGroups.PERSIST, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>기본값과 같아도 못박혀 있어야 한다.</b> 이 둘은 단순 튜닝 값이 아니라
     * <b>한 묶음의 처리 시간 예산</b>이고, {@code HttpNotificationSender} 가 여기서
     * 건당 상한을 계산해 간다. 설정 맵에 안 실리면 클라이언트 판이 올라가며 값이 조용히
     * 바뀔 수 있고, 그 순간 그 상한이 근거 없는 숫자가 된다.
     *
     * <p>기본값이 마침 같다는 것은 <b>실측했다</b>({@code kafka-clients} 4.2.1:
     * {@code max.poll.records=500}, {@code max.poll.interval.ms=300000}). 같다는 사실이
     * 안 적어도 된다는 뜻은 아니다 — 같다는 것 자체가 다음 판에서 안 같아질 수 있다.
     */
    @Test
    @DisplayName("한 묶음의 처리 시간 예산은 기본값에 맡기지 않고 설정 맵에 싣는다")
    void pollBudgetIsPinnedInTheConfigInsteadOfLeftToDefaults() {
        Map<String, Object> config =
                KafkaConsumerGroups.consumerConfig(KafkaConsumerGroups.NOTIFY_DISPATCH, "localhost:9094");

        assertThat(config.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG))
                .as("한 poll 이 가져오는 건수 — 건당 상한의 분모다")
                .isEqualTo((int) KafkaConsumerGroups.MAX_POLL_RECORDS);
        assertThat(config.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG))
                .as("그 묶음을 처리할 시간 — 건당 상한의 분자다")
                .isEqualTo((int) KafkaConsumerGroups.MAX_POLL_INTERVAL_MILLIS);
    }

    /**
     * 예산이 <b>나눠떨어져야</b> 유도한 상한이 실제 최악을 덮는다. 예컨대 500 건에 601ms 를
     * 허용하면 최악이 {@code 300.5초} 로 5분을 <b>넘는다</b> — 소수점 아래를 버리는 나눗셈이
     * 상한을 과대평가하지 않는지 여기서 잡는다.
     */
    @Test
    @DisplayName("건당 상한 × 묶음 건수가 poll 간격을 넘지 않는다")
    void derivedPerRecordCapCoversTheWholeBatch() {
        long perRecord =
                KafkaConsumerGroups.MAX_POLL_INTERVAL_MILLIS / KafkaConsumerGroups.MAX_POLL_RECORDS;

        assertThat(perRecord * KafkaConsumerGroups.MAX_POLL_RECORDS)
                .as("건당 상한을 꽉 채워 쓴 최악이 poll 간격 안이어야 한다")
                .isLessThanOrEqualTo(KafkaConsumerGroups.MAX_POLL_INTERVAL_MILLIS);
    }
}
