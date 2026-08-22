package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaAdmin;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * 토픽이 <b>선언대로 만들어졌는지</b> 확인하는 자리를 지킨다.
 *
 * <p>Spring 의 어드민은 이미 있는 토픽에 대해 파티션 수와 설정만 맞추고 <b>복제본 수는 비교하지
 * 않는다</b>. 그래서 브로커가 자동 생성한 RF1 토픽에도 성공을 돌려준다 — 그 상태가 정확히
 * 프로비저닝 지표가 잡으려던 사고다. RF 는 만들 때만 정해지고 나중에는 파티션 재배치라는 별도
 * 운영 작업으로만 바뀐다.
 *
 * <p>이 검증이 없으면 지표가 초록인데 계약은 깨져 있고, 브로커 한 대가 빠지는 순간 그 파티션의
 * 발급건이 사라진다.
 */
@ExtendWith(OutputCaptureExtension.class)
class TopicDeclarationVerificationTest {

    private static final int DECLARED_PARTITIONS = 6;

    @Test
    @DisplayName("선언대로면 통과한다")
    void acceptsTopicsThatMatchTheDeclaration() {
        assertThat(KafkaTopicConfig.verifyDeclaration(describing(asDeclared()), declaredFloors()::get))
                .isEqualTo(ProvisionOutcome.PROVISIONED);
    }

    @Test
    @DisplayName("브로커가 RF1 로 만들어 둔 토픽을 통과시키지 않는다")
    void rejectsAutoCreatedTopicsWithTooFewReplicas() {
        Map<String, TopicDescription> broker = asDeclared();
        broker.put(KafkaTopicConfig.ISSUE_PERSIST,
                describe(KafkaTopicConfig.ISSUE_PERSIST, DECLARED_PARTITIONS, 1));

        assertThat(KafkaTopicConfig.verifyDeclaration(describing(broker), declaredFloors()::get))
                .as("RF1 은 되돌릴 수 없다. 여기서 못 잡으면 지표가 초록인데 계약이 깨진 채 돈다")
                .isEqualTo(ProvisionOutcome.MISMATCHED);
    }

    @Test
    @DisplayName("DLT 도 같은 기준으로 본다 — 격리본이 복제되지 않으면 잃어버린다")
    void deadLetterTopicsAreVerifiedToo() {
        Map<String, TopicDescription> broker = asDeclared();
        broker.put(KafkaTopicConfig.NOTIFY_DLT,
                describe(KafkaTopicConfig.NOTIFY_DLT, DECLARED_PARTITIONS, 1));

        assertThat(KafkaTopicConfig.verifyDeclaration(describing(broker), declaredFloors()::get))
                .isEqualTo(ProvisionOutcome.MISMATCHED);
    }

    @Test
    @DisplayName("파티션이 부족해도 통과시키지 않는다 — 컨슈머 동시성이 거기 묶여 있다")
    void rejectsTopicsWithTooFewPartitions() {
        Map<String, TopicDescription> broker = asDeclared();
        broker.put(KafkaTopicConfig.ISSUE_ATTEMPT, describe(KafkaTopicConfig.ISSUE_ATTEMPT, 1, 2));

        assertThat(KafkaTopicConfig.verifyDeclaration(describing(broker), declaredFloors()::get))
                .isEqualTo(ProvisionOutcome.MISMATCHED);
    }

    /** attempt 만 RF2 다. 그보다 많은 것은 문제가 아니다 — 부족한 것만 문제다. */
    @Test
    @DisplayName("선언보다 복제본이 많은 것은 통과시킨다")
    void moreReplicasThanDeclaredIsFine() {
        Map<String, TopicDescription> broker = asDeclared();
        broker.put(KafkaTopicConfig.ISSUE_ATTEMPT, describe(KafkaTopicConfig.ISSUE_ATTEMPT,
                DECLARED_PARTITIONS, 3));

        assertThat(KafkaTopicConfig.verifyDeclaration(describing(broker), declaredFloors()::get))
                .isEqualTo(ProvisionOutcome.PROVISIONED);
    }

    /**
     * 검증 메서드가 <b>판정에 실제로 쓰이는지</b> 본다. 이 결합을 안 지키면
     * {@code && matchesDeclaration(...)} 한 조각만 지워도 위 테스트들이 전부 초록이라,
     * 아무도 모르게 "만들었으면 성공" 으로 되돌아간다.
     */
    @Test
    @DisplayName("생성에 성공해도 선언과 다르면 프로비저닝 성공이 아니다")
    void creationAloneIsNotSuccess() {
        Map<String, TopicDescription> broker = asDeclared();
        broker.put(KafkaTopicConfig.ISSUE_PERSIST,
                describe(KafkaTopicConfig.ISSUE_PERSIST, DECLARED_PARTITIONS, 1));

        assertThat(KafkaTopicConfig.provisioningStep(() -> true, describing(broker), declaredFloors()::get).get())
                .as("만들기는 했는데 RF1 이다 — 재시도가 고치지 못하니 불일치로 끝내야 한다")
                .isEqualTo(ProvisionOutcome.MISMATCHED);
        assertThat(KafkaTopicConfig.provisioningStep(() -> true, describing(asDeclared()), declaredFloors()::get).get())
                .isEqualTo(ProvisionOutcome.PROVISIONED);
    }

    @Test
    @DisplayName("만들지 못했으면 확인하러 가지 않는다")
    void doesNotVerifyWhenCreationFailed() {
        KafkaAdmin exploding = new KafkaAdmin(Map.of()) {
            @Override
            public Map<String, TopicDescription> describeTopics(String... topicNames) {
                throw new AssertionError("생성이 실패했는데 확인을 시도했다");
            }
        };

        assertThat(KafkaTopicConfig.provisioningStep(() -> false, exploding, declaredFloors()::get).get())
                .as("만들지 못한 것은 불일치가 아니다 — 브로커가 늦게 뜨면 재시도가 고친다")
                .isEqualTo(ProvisionOutcome.UNCONFIRMED);
    }

    @Test
    @DisplayName("브로커가 답하지 않은 토픽이 있으면 통과시키지 않는다")
    void rejectsWhenABrokerAnswerIsMissing() {
        Map<String, TopicDescription> broker = asDeclared();
        broker.remove(KafkaTopicConfig.NOTIFY);

        assertThat(KafkaTopicConfig.verifyDeclaration(describing(broker), declaredFloors()::get))
                .as("응답이 빠진 것은 불일치가 아니라 미확인이다 — 재시도가 고칠 수 있다."
                        + " MISMATCHED 로 분류하면 재기동하면 나을 상황에 '재배치하라' 가 나간다")
                .isEqualTo(ProvisionOutcome.UNCONFIRMED);
    }

    /**
     * 위의 "응답이 빠진다" 는 <b>실제로는 일어나지 않는 모양</b>이다. 스프링의 어드민은 없는
     * 토픽을 부분 맵으로 돌려주지 않고 {@code UnknownTopicOrPartitionException} 을 감싼
     * {@code KafkaException} 을 던진다. 그 예외가 그냥 위로 새면 재시도 루프가 스택트레이스만
     * 남기고, <b>어떤 토픽이 없는지</b>는 아무 데도 안 남는다.
     */
    @Test
    @DisplayName("브로커가 토픽이 없다고 예외를 던지면 어떤 토픽인지 남기고 미확인으로 끝낸다")
    void translatesTheMissingTopicException(CapturedOutput output) {
        KafkaAdmin missing = new KafkaAdmin(Map.of()) {
            @Override
            public Map<String, TopicDescription> describeTopics(String... topicNames) {
                throw new KafkaException("Failed to describe topics",
                        new UnknownTopicOrPartitionException("This server does not host this topic-partition."));
            }
        };

        assertThat(KafkaTopicConfig.verifyDeclaration(missing, declaredFloors()::get))
                .as("토픽이 아직 없는 것은 불일치가 아니다 — 재시도가 고친다")
                .isEqualTo(ProvisionOutcome.UNCONFIRMED);
        assertThat(output)
                .as("어떤 토픽이 없는지 안 남기면 다음 사람이 브로커부터 다시 뒤진다")
                .contains(KafkaTopicConfig.ISSUE_PERSIST);
    }

    /**
     * {@code acks=all} 이 "복제본 한 대에만 썼는데 성공" 이 되지 않게 하는 것은
     * {@code min.insync.replicas=2} 다. 파티션·복제본만 보면 누군가 장애 대응 중 ISR 하한을
     * 1 로 낮춰 두고 되돌리지 않아도 지표는 <b>반영됨(1)</b> 이다 — 그 구간에 리더가 빠지면
     * 그 파티션의 persist 레코드가 사라진다.
     *
     * <p>설정은 되돌릴 수 있으므로 {@code MISMATCHED}(재기동으로 안 낫는다)가 아니라
     * {@code UNCONFIRMED} 다 — 다음 재시도의 {@code initialize()} 가 고칠 수 있다.
     */
    @Test
    @DisplayName("ISR 하한이 선언보다 낮으면 통과시키지 않는다")
    void rejectsTopicsWhoseInSyncFloorWasLowered() {
        Map<String, String> loosened = declaredFloors();
        loosened.put(KafkaTopicConfig.ISSUE_PERSIST, "1");

        assertThat(KafkaTopicConfig.verifyInSyncFloor(loosened::get))
                .as("acks=all 이 복제본 한 대에만 쓰고 성공하는 구간이 열린다")
                .isEqualTo(ProvisionOutcome.UNCONFIRMED);
        assertThat(KafkaTopicConfig.verifyInSyncFloor(declaredFloors()::get))
                .isEqualTo(ProvisionOutcome.PROVISIONED);
    }

    @Test
    @DisplayName("설정을 못 읽으면 통과가 아니라 미확인이다")
    void unreadableConfigsAreNotSuccess() {
        assertThat(KafkaTopicConfig.verifyInSyncFloor(topic -> null))
                .as("못 읽은 것을 통과시키면 이 검증이 통째로 공허해진다")
                .isEqualTo(ProvisionOutcome.UNCONFIRMED);
    }

    /** attempt 는 acks=0 이라 ISR 하한을 선언하지 않는다. 없다고 실패로 보면 안 된다. */
    @Test
    @DisplayName("ISR 하한을 선언하지 않은 토픽은 검사 대상이 아니다")
    void topicsWithoutAnInSyncFloorAreSkipped() {
        Map<String, String> floors = declaredFloors();
        floors.remove(KafkaTopicConfig.ISSUE_ATTEMPT);

        assertThat(KafkaTopicConfig.verifyInSyncFloor(floors::get))
                .isEqualTo(ProvisionOutcome.PROVISIONED);
    }

    /**
     * ISR 검사가 <b>판정에 실제로 쓰이는지</b> 본다. 이 결합을 안 지키면
     * {@code return verifyInSyncFloor(floorOf)} 한 줄만 지워도 위 테스트들이 전부 초록이라,
     * 아무도 모르게 "파티션·복제본만 맞으면 성공" 으로 되돌아간다.
     */
    @Test
    @DisplayName("파티션·복제본이 맞아도 ISR 하한이 낮으면 성공이 아니다")
    void topologyAloneIsNotSuccess() {
        Map<String, String> loosened = declaredFloors();
        loosened.put(KafkaTopicConfig.ISSUE_PERSIST, "1");

        assertThat(KafkaTopicConfig.verifyDeclaration(describing(asDeclared()), loosened::get))
                .as("여기서 안 잡으면 acks=all 이 복제본 한 대에만 쓰고 성공하는 구간이 초록으로 남는다")
                .isEqualTo(ProvisionOutcome.UNCONFIRMED);
    }

    private static Map<String, String> declaredFloors() {
        return KafkaTopicConfig.allTopics().stream()
                .filter(topic -> !KafkaTopicConfig.ISSUE_ATTEMPT.equals(topic))
                .collect(Collectors.toMap(Function.identity(), topic -> "2"));
    }

    private static Map<String, TopicDescription> asDeclared() {
        return KafkaTopicConfig.allTopics().stream()
                .collect(Collectors.toMap(Function.identity(), topic -> describe(topic,
                        DECLARED_PARTITIONS,
                        KafkaTopicConfig.ISSUE_ATTEMPT.equals(topic) ? 2 : 3)));
    }

    private static TopicDescription describe(String topic, int partitions, int replicas) {
        List<Node> nodes = IntStream.range(0, replicas)
                .mapToObj(id -> new Node(id, "broker-" + id, 9092))
                .toList();
        List<TopicPartitionInfo> partitionInfos = IntStream.range(0, partitions)
                .mapToObj(partition -> new TopicPartitionInfo(partition, nodes.get(0), nodes, nodes))
                .toList();
        return new TopicDescription(topic, false, partitionInfos);
    }

    /** {@code describeTopics} 만 대신 답하는 어드민. 브로커도 네트워크도 필요 없다. */
    private static KafkaAdmin describing(Map<String, TopicDescription> described) {
        return new KafkaAdmin(Map.of()) {
            @Override
            public Map<String, TopicDescription> describeTopics(String... topicNames) {
                return described;
            }
        };
    }
}
