package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * OBS-35. <b>{@link KafkaTopicConfig} 의 선언과 {@code compose.yml} 이 띄우는 브로커를 잇는다.</b>
 *
 * <p>이 계약은 두 파일에 걸쳐 있고, <b>각각을 따로 보는 테스트로는 못 지킨다</b> — 선언은 혼자서
 * 항상 유효하고(RF3 은 그냥 숫자다), compose 도 혼자서 항상 유효하다(브로커 1대짜리 compose 는
 * 문법이 맞고 잘 뜬다). 깨지는 것은 둘이 만나는 자리뿐이고, 그 증상은 기동 실패가 아니라
 * 토픽 생성이 {@code INVALID_REPLICATION_FACTOR} 로 조용히 재시도를 반복하는 상태다.
 * 앱은 멀쩡히 떠 있고 발급도 되므로 아무도 모른다.
 *
 * <p><b>잡지 못하는 것 ①</b> — 실제로 브로커가 세 대 <em>떠 있는지</em>. 이 테스트가 보는 것은
 * 선언뿐이다. 기동 확인은 {@code app_kafka_topics_provisioned} 지표가 한다.
 *
 * <p><b>잡지 못하는 것 ②</b> — 커밋되지 않는 {@code .env} 의 값. 브로커 설정은 전부 compose 에
 * literal 로 박혀 있어 {@code .env} 가 못 바꾸지만, {@code KAFKA_ENABLED} 는 거기서 온다.
 */
class KafkaBrokerComposeContractTest {

    private static final String COMPOSE_FILE = "compose.yml";

    /** 브로커 안쪽 리스너 포트. compose 내부 전용이라 호스트 매핑이 없다. */
    private static final String BROKER_PORT = "9092";

    /**
     * 스토리지 포맷에 쓰이는 변수. <b>{@code KAFKA_} 접두사가 없다</b> — 이미지가
     * {@code KAFKA_*} 를 {@code server.properties} 로 옮기는 것은 포맷이 끝난 뒤라,
     * 포맷 단계는 이 이름 하나만 읽는다({@code /etc/kafka/docker/launch}).
     */
    private static final String CLUSTER_ID = "CLUSTER_ID";

    /** 같은 뜻으로 착각하기 쉬운 이름. 적으면 조용히 무시된다. */
    private static final String PREFIXED_CLUSTER_ID = "KAFKA_" + CLUSTER_ID;

    @Test
    @DisplayName("브로커 수가 선언된 최대 복제본 수 이상이다 — 모자라면 토픽 생성이 영영 실패한다")
    void theBrokerCountCoversTheHighestDeclaredReplicationFactor() throws IOException {
        int highestReplicationFactor = declaredTopics().stream()
                .mapToInt(NewTopic::replicationFactor)
                .max()
                .orElseThrow();

        assertThat(brokers().size())
                .as("선언이 RF %d 를 요구한다. compose 가 그보다 적은 브로커를 띄우면 토픽 생성이"
                        + " INVALID_REPLICATION_FACTOR 로 실패하고, 프로비저너는 물러서며 영영"
                        + " 재시도한다 — 앱은 뜨고 발급도 되므로 로그를 안 보면 아무도 모른다."
                        + " 파티션·복제 수는 발표 회차의 값이라 로컬 편의로 낮추지 않는다",
                        highestReplicationFactor)
                .isGreaterThanOrEqualTo(highestReplicationFactor);
    }

    @Test
    @DisplayName("min.insync.replicas 선언을 만족할 브로커가 남는다 — RF 와 ISR 은 다른 수다")
    void theBrokerCountLeavesRoomForTheDeclaredInSyncFloor() throws IOException {
        int floor = declaredTopics().stream()
                .mapToInt(KafkaBrokerComposeContractTest::declaredInSyncFloor)
                .max()
                .orElseThrow();

        // RF 만 맞추면 "만들어지긴 하는데 acks=all 쓰기가 한 대 죽는 순간 전부 실패" 하는
        // 클러스터가 된다. ISR 하한이 2면 브로커가 최소 2대는 살아 있어야 하고, 그 여유는
        // 대수가 하한보다 커야 생긴다.
        assertThat(brokers().size())
                .as("선언된 min.insync.replicas 최대값이 %d 다. 브로커가 그 수와 같으면 한 대만"
                        + " 빠져도 acks=all 인 coupon.issue.persist 쓰기가 전부 실패한다", floor)
                .isGreaterThan(floor);
    }

    @Test
    @DisplayName("모든 브로커가 auto.create.topics.enable 을 끈다 — 켜지면 RF1 토픽이 되돌릴 수 없이 생긴다")
    void everyBrokerRefusesToCreateTopicsOnItsOwn() throws IOException {
        List<String> offenders = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> broker : brokers().entrySet()) {
            Object value = environmentOf(broker.getValue()).get("KAFKA_AUTO_CREATE_TOPICS_ENABLE");
            if (!"false".equals(String.valueOf(value))) {
                offenders.add(broker.getKey() + " → " + value);
            }
        }

        assertThat(offenders)
                .as("켜져 있으면 프로비저닝보다 첫 발급이 빨랐을 때 브로커가 RF1 · 파티션1 짜리"
                        + " 토픽을 대신 만든다. 복제본 수는 만들 때만 정해져 나중에 못 고치므로,"
                        + " 노드 하나가 빠지면 그 파티션이 사라져 영구 미영속 발급이 된다."
                        + " 이건 기본값이 true 라 '안 적으면 꺼짐' 이 아니다")
                .isEmpty();
    }

    @Test
    @DisplayName("브로커를 호스트에 열지 않는다 — 인증이 없어 여는 순간 아무나 이벤트를 위조한다")
    void noBrokerPublishesAHostPort() throws IOException {
        List<String> exposed = brokers().entrySet().stream()
                .filter(broker -> broker.getValue().get("ports") != null)
                .map(Map.Entry::getKey)
                .toList();

        assertThat(exposed)
                .as("Kafka 에 인증이 없다(SASL 미도입). 호스트로 여는 순간 같은 네트워크의"
                        + " 아무나 coupon.issue.persist 에 발급 이벤트를 밀어 넣을 수 있고,"
                        + " 그건 DB 정합성으로 이어진다 — 관리 포트·Prometheus 를 잠근 것과"
                        + " 같은 이유다. 대가: 호스트에서 IDE 로 띄운 앱은 브로커에 닿지 못하므로"
                        + " 그 회차는 KAFKA_ENABLED=false 로 둔다")
                .isEmpty();
    }

    @Test
    @DisplayName("api 가 보는 부트스트랩 주소가 실제 브로커 서비스 이름과 같다")
    void theApiBootstrapAddressesResolveToTheDeclaredBrokers() throws IOException {
        Map<String, Object> api = serviceNamed("api");
        Object bootstrap = environmentOf(api).get("KAFKA_BOOTSTRAP_SERVERS");

        assertThat(bootstrap)
                .as(".env 의 KAFKA_BOOTSTRAP_SERVERS 는 호스트 실행용 값이라 컨테이너 안에서는"
                        + " 틀리다(localhost 는 api 자신이다). compose 가 서비스 DNS 로 덮지"
                        + " 않으면 KAFKA_ENABLED=true 인 순간 조용히 재시도만 쌓인다")
                .isNotNull();

        Set<String> addressed = new TreeSet<>();
        for (String address : String.valueOf(bootstrap).split(",")) {
            String trimmed = address.strip();
            assertThat(trimmed)
                    .as("부트스트랩 주소는 브로커 리스너 포트(%s)를 가리켜야 한다", BROKER_PORT)
                    .endsWith(":" + BROKER_PORT);
            addressed.add(trimmed.substring(0, trimmed.lastIndexOf(':')));
        }

        assertThat(addressed)
                .as("한 대만 적으면 그 한 대가 재기동 중일 때 부트스트랩이 실패한다."
                        + " 서비스 이름을 바꾸고 여기를 안 고치면 DNS 가 안 풀려 같은 증상이 된다")
                .isEqualTo(new TreeSet<>(brokers().keySet()));
    }

    @Test
    @DisplayName("각 브로커가 자기 서비스 이름을 광고한다 — localhost 를 광고하면 api 가 자신에게 붙는다")
    void everyBrokerAdvertisesItsOwnServiceName() throws IOException {
        for (Map.Entry<String, Map<String, Object>> broker : brokers().entrySet()) {
            String advertised = String.valueOf(
                    environmentOf(broker.getValue()).get("KAFKA_ADVERTISED_LISTENERS"));

            // 부트스트랩 주소만 보는 것으로는 부족하다. 클라이언트는 부트스트랩으로 메타데이터를
            // 받은 뒤 <b>거기 적힌 주소</b>로 다시 붙는다 — 광고 주소가 localhost 면 부트스트랩은
            // 성공하고 그다음 연결이 api 컨테이너 자신을 향한다. 증상은 연결 거부가 아니라
            // "브로커가 있는데 아무것도 안 되는" 상태다.
            assertThat(advertised)
                    .as("%s 는 compose 네트워크 안에서 자기 서비스 이름으로 닿아야 한다."
                            + " 이름이 틀리면 DNS 가 안 풀리고, localhost 면 부르는 쪽 자신에게 간다",
                            broker.getKey())
                    .isEqualTo("PLAINTEXT://" + broker.getKey() + ":" + BROKER_PORT);

            // 광고와 실제 바인딩은 다른 값이다. 광고만 맞고 리스너가 다른 포트에 붙으면
            // 부트스트랩도 메타데이터도 통과한 뒤 연결에서만 거부된다.
            //
            // ⚠️ 항목 전체를 값으로 비교한다. 부분 문자열로 찾으면 PLAINTEXT://:90920 이
            //    통과하고, 포트만 떼어 보면 PLAINTEXT://localhost:9092 가 통과한다 —
            //    후자는 컨테이너 안 루프백에만 붙어서 다른 컨테이너가 영영 못 닿는다.
            //    호스트가 비어 있어야 모든 인터페이스에 붙는다.
            assertThat(plaintextListener(broker.getValue()))
                    .as("%s 는 모든 인터페이스의 %s 포트에 붙어야 한다", broker.getKey(), BROKER_PORT)
                    .isEqualTo("PLAINTEXT://:" + BROKER_PORT);
        }
    }

    @Test
    @DisplayName("컨트롤러 쿼럼 명단이 실제 브로커 셋과 일치하고 노드 id 가 겹치지 않는다")
    void theControllerQuorumMatchesTheBrokersThatExist() throws IOException {
        Map<String, Map<String, Object>> brokers = brokers();

        Set<String> nodeIds = new TreeSet<>();
        Set<String> clusterIds = new TreeSet<>();
        Set<String> quorums = new TreeSet<>();
        for (Map<String, Object> broker : brokers.values()) {
            Map<String, Object> environment = environmentOf(broker);
            nodeIds.add(String.valueOf(environment.get("KAFKA_NODE_ID")));
            clusterIds.add(String.valueOf(environment.get(CLUSTER_ID)));
            quorums.add(String.valueOf(environment.get("KAFKA_CONTROLLER_QUORUM_VOTERS")));
        }

        assertThat(nodeIds)
                .as("두 대가 같은 node id 를 쓰면 나중에 뜬 쪽이 기동에 실패한다."
                        + " 남은 두 대로 쿼럼이 서기 때문에 클러스터는 정상으로 보인다")
                .hasSize(brokers.size());

        assertThat(clusterIds)
                .as("cluster id 가 서로 다르면 컨트롤러가 상대를 '다른 클러스터' 로 보고 붙지"
                        + " 않는다. 한 번 포맷된 볼륨의 id 는 바뀌지 않으므로 이 값을 고칠 때는"
                        + " 볼륨도 함께 지워야 한다")
                .hasSize(1);

        assertThat(clusterIds.iterator().next())
                .as("포맷에 쓰이는 이름은 %s 다. 안 적으면 이미지 기본값으로 포맷되는데,"
                        + " 세 대가 그 기본값을 똑같이 쓰므로 클러스터는 정상으로 보인다 —"
                        + " 즉 이 값을 빠뜨린 것이 증상으로 드러나지 않는다(실측)", CLUSTER_ID)
                .isNotEqualTo("null");

        assertThat(quorums)
                .as("쿼럼 명단이 브로커마다 다르면 서로 다른 쿼럼을 기다린다")
                .hasSize(1);

        Set<String> voterHosts = new TreeSet<>();
        Set<String> voterIds = new TreeSet<>();
        for (String voter : quorums.iterator().next().split(",")) {
            String[] parts = voter.strip().split("@");
            voterIds.add(parts[0]);
            voterHosts.add(parts[1].substring(0, parts[1].lastIndexOf(':')));
        }

        assertThat(voterHosts)
                .as("쿼럼 명단이 없는 서비스를 가리키면 컨트롤러가 영영 선출되지 않는다 —"
                        + " 브로커 소켓은 열려 있어서 포트만 보는 헬스체크는 초록불이다")
                .isEqualTo(new TreeSet<>(brokers.keySet()));

        assertThat(voterIds)
                .as("쿼럼의 node id 명단이 실제 브로커의 id 와 같아야 한다")
                .isEqualTo(nodeIds);
    }

    @Test
    @DisplayName("KAFKA_CLUSTER_ID 로 적지 않는다 — 접두사가 붙으면 조용히 무시된다")
    void noBrokerUsesThePrefixedClusterIdName() throws IOException {
        List<String> offenders = brokers().entrySet().stream()
                .filter(broker -> environmentOf(broker.getValue()).containsKey(PREFIXED_CLUSTER_ID))
                .map(Map.Entry::getKey)
                .toList();

        // 실측 — KAFKA_CLUSTER_ID 로 적으면 브로커가 모르는 cluster.id 프로퍼티가
        // server.properties 에 생기고, 포맷은 이미지 기본값으로 진행된다. 그러면
        // 설정 파일의 cluster.id 와 meta.properties 의 cluster.id 가 서로 다른 채로 뜬다.
        // 기동도 되고 세 대가 붙기까지 하므로 이 실수는 증상이 없다.
        assertThat(offenders)
                .as("%s 는 이미지가 %s 로 옮기는 이름이고, 스토리지 포맷은 그보다 앞이라 그"
                        + " 프로퍼티를 안 본다. 포맷에 쓰이는 이름은 접두사 없는 %s 다",
                        PREFIXED_CLUSTER_ID, "server.properties 의 cluster.id", CLUSTER_ID)
                .isEmpty();
    }

    @Test
    @DisplayName("내부 토픽 복제도 브로커 수에 맞춘다 — 1로 떨어지면 컨슈머 offset 이 통째로 날아간다")
    void theInternalTopicsAreReplicatedToo() throws IOException {
        Map<String, Map<String, Object>> brokers = brokers();
        for (Map.Entry<String, Map<String, Object>> broker : brokers.entrySet()) {
            assertThat(environmentOf(broker.getValue()).get("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"))
                    .as("%s 의 __consumer_offsets 복제본 수. 1 이면 그 브로커가 빠질 때 컨슈머"
                            + " 그룹 offset 이 사라지고 OBS-15 의 컨슈머가 처음부터 다시 읽는다",
                            broker.getKey())
                    .isEqualTo(String.valueOf(brokers.size()));
        }
    }

    /**
     * {@code KAFKA_LISTENERS} 에서 {@code PLAINTEXT} 항목을 <b>통째로</b> 돌려준다.
     * 값은 {@code PLAINTEXT://:9092,CONTROLLER://:9093} 처럼 쉼표로 이어진 목록이다.
     *
     * <p>포트만 떼어 내지 않는 이유 — 바인드 호스트도 계약이다. 호스트가 비어 있어야 모든
     * 인터페이스에 붙고, {@code localhost} 면 컨테이너 안에서만 닿는다.
     */
    private String plaintextListener(Map<String, Object> broker) {
        String listeners = String.valueOf(environmentOf(broker).get("KAFKA_LISTENERS"));
        for (String listener : listeners.split(",")) {
            String trimmed = listener.strip();
            if (trimmed.startsWith("PLAINTEXT://")) {
                return trimmed;
            }
        }
        throw new AssertionError("KAFKA_LISTENERS 에 PLAINTEXT 리스너가 없다: " + listeners);
    }

    /**
     * 선언된 토픽을 <b>빈 메서드에서 직접 얻는다.</b> 이름 목록을 옮겨 적으면 토픽이 하나 늘 때
     * 이 계약이 조용히 그 하나를 안 본다 — 마침 그 하나가 RF 를 올린 토픽이면 가드가 무의미해진다.
     *
     * <p>{@link KafkaTopicConfig} 에 목록용 API 를 새로 열지 않는 이유는 그 파일이 OBS-17
     * 소유이기 때문이다. 리플렉션은 그 경계를 넘지 않으면서 같은 값을 얻는 방법이다.
     */
    private static List<NewTopic> declaredTopics() {
        KafkaTopicConfig config = new KafkaTopicConfig();
        List<NewTopic> topics = new ArrayList<>();
        for (var method : KafkaTopicConfig.class.getDeclaredMethods()) {
            if (method.getReturnType() != NewTopic.class || method.getParameterCount() != 0) {
                continue;
            }
            try {
                method.setAccessible(true);
                topics.add((NewTopic) method.invoke(config));
            } catch (ReflectiveOperationException failure) {
                throw new AssertionError("토픽 선언을 읽지 못했다: " + method.getName(), failure);
            }
        }
        assertThat(topics)
                .as("KafkaTopicConfig 에서 NewTopic 선언을 하나도 찾지 못했다 —"
                        + " 이 상태로 통과하면 아래 단언이 전부 헛것이 된다")
                .isNotEmpty();
        return topics;
    }

    /**
     * 선언에 {@code min.insync.replicas} 가 없으면 브로커 기본값(1)이다. coupon.issue.attempt 가
     * 그 경우인데, {@code acks=0} 이라 ISR 하한이 발행 경로에 아무 영향을 주지 않는다.
     */
    private static int declaredInSyncFloor(NewTopic topic) {
        Map<String, String> configs = topic.configs();
        String floor = configs == null ? null : configs.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        return floor == null ? 1 : Integer.parseInt(floor);
    }

    // ── 읽기 도우미 ───────────────────────────────────────────────────────────

    /**
     * 브로커 서비스. 이름으로 고르지 않고 <b>Kafka 이미지를 쓰는 서비스</b>로 고른다 —
     * 이름 규칙으로 고르면 서비스 하나를 {@code broker-4} 로 추가했을 때 계약 밖으로 새어 나가고,
     * 그 사실이 이 테스트에서는 초록불로 보인다.
     */
    private Map<String, Map<String, Object>> brokers() throws IOException {
        Map<String, Map<String, Object>> found = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : services().entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> definition = (Map<String, Object>) entry.getValue();
            String image = String.valueOf(definition.get("image"));
            if (image.contains("kafka")) {
                found.put(entry.getKey(), definition);
            }
        }
        assertThat(found).as("compose 에 Kafka 브로커 서비스가 하나도 없다").isNotEmpty();
        return found;
    }

    private Map<String, Object> serviceNamed(String service) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> definition = (Map<String, Object>) services().get(service);
        assertThat(definition).as("compose 에 %s 서비스가 없다", service).isNotNull();
        return definition;
    }

    private Map<String, Object> environmentOf(Map<String, Object> service) {
        @SuppressWarnings("unchecked")
        Map<String, Object> environment = (Map<String, Object>) service.get("environment");
        return environment == null ? Map.of() : environment;
    }

    private Map<String, Object> services() throws IOException {
        Path compose = repoRoot().resolve(COMPOSE_FILE);
        if (!Files.isRegularFile(compose)) {
            throw new AssertionError("계약에 걸린 파일이 없다: " + compose);
        }
        try (var in = Files.newInputStream(compose)) {
            Map<String, Object> document = new Yaml().load(in);
            @SuppressWarnings("unchecked")
            Map<String, Object> services = (Map<String, Object>) document.get("services");
            return services;
        }
    }

    private static Path repoRoot() {
        Path candidate = Path.of("").toAbsolutePath();
        while (candidate != null) {
            if (Files.isRegularFile(candidate.resolve("settings.gradle"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        throw new IllegalStateException("저장소 루트를 찾지 못했다");
    }
}
