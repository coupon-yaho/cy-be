package com.kafkick.infra.mq.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * 토픽 3 + DLT 2 를 선언한다. 컨슈머와 적재는 OBS-15 몫이고 여기는 선언까지다.
 *
 * <table>
 *   <caption>토픽별 내구성 등급</caption>
 *   <tr><th>토픽</th><th>acks</th><th>RF / ISR</th><th>보존</th><th>범위</th></tr>
 *   <tr><td>{@code coupon.issue.persist}</td><td>all</td><td>3 / 2</td><td>7일</td><td>v3 만</td></tr>
 *   <tr><td>{@code coupon.issue.attempt}</td><td>0</td><td>2 / -</td><td>12시간 · 2GiB</td><td>전 버전</td></tr>
 *   <tr><td>{@code coupon.notify}</td><td>all</td><td>3 / 2</td><td>7일</td><td>-</td></tr>
 * </table>
 *
 * <p>attempt 에만 {@code min.insync.replicas} 가 없다. {@code acks=0} 은 리더의 응답조차
 * 기다리지 않아서 ISR 하한이 발행 경로에 아무 영향을 주지 못한다 — 적어 두면 지켜지는 것처럼
 * 읽히기만 한다.
 *
 * <h2>파티션은 6이다</h2>
 *
 * 처리량 때문이 아니다. ① 파티션 수는 줄일 수 없어 나중에 되돌리지 못하고 ② 리스너의
 * {@code concurrency} 가 파티션 수를 넘을 수 없으며(6 파티션에 8을 주면 2개가 논다)
 * ③ 3의 배수라 브로커 3대에 균등하게 떨어진다.
 *
 * <p><b>DLT 도 파티션이 6이다.</b> Spring 의 {@code DeadLetterPublishingRecoverer} 는 기본적으로
 * 원본과 <b>같은 번호</b>의 파티션으로 보낸다. DLT 파티션이 더 적으면 그 번호가 없어서 격리하려던
 * 레코드가 발행에서 실패한다 — poison message 를 치우려다 컨슈머가 다시 멈춘다.
 *
 * <h2>보존 상한을 브로커 기본값에 맡기지 않는다</h2>
 *
 * attempt 는 초당 수천 건이 쌓이는데 아무도 과거를 읽지 않는다({@code auto.offset.reset=latest}).
 * 상한이 없으면 <b>아무도 안 읽는 로그가 디스크를 채우고, 디스크가 차면 {@code acks=all} 인
 * persist 쓰기가 먼저 실패한다</b> — 관측 토픽이 정합성 토픽을 죽이는 구조다.
 *
 * <p>{@code retention.bytes} 는 <b>파티션당</b>이다. 여기 적은 2GiB 는 파티션 6개를 곱해
 * 토픽 총량 12GiB 를 뜻한다. 이 값을 바꿀 때 6배를 잊지 말 것.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("kafka.enabled")
@EnableConfigurationProperties(KafkaConnectionProperties.class)
public class KafkaTopicConfig {

    public static final String ISSUE_PERSIST = "coupon.issue.persist";
    public static final String ISSUE_ATTEMPT = "coupon.issue.attempt";
    public static final String NOTIFY = "coupon.notify";

    /** Spring 의 기본 DLT 접미사와 같은 표기다. 다르게 적으면 격리본이 선언되지 않은 토픽으로 간다. */
    public static final String DLT_SUFFIX = ".DLT";

    public static final String ISSUE_PERSIST_DLT = ISSUE_PERSIST + DLT_SUFFIX;
    public static final String NOTIFY_DLT = NOTIFY + DLT_SUFFIX;

    /**
     * DLT 를 두지 않는 토픽. attempt 는 관측 로그라 한 건 버려도 회차 판정이 안 바뀌고, 격리 구역을
     * 유지하는 비용이 그 이득보다 크다 — 대신 컨슈머는 카운터를 올리고 offset 을 넘긴다.
     *
     * <p>Spring 의 기본 목적지 해석기는 원본 이름 + {@code .DLT} 라, attempt 컨슈머에 공용 에러
     * 핸들러를 달면 <b>선언하지 않은 토픽이 브로커에서 RF1 으로 생긴다.</b> 그 토픽은
     * {@link #allTopics()} 밖이라 선언 검증도 영원히 못 본다.
     */
    public static final Set<String> TOPICS_WITHOUT_DLT = Set.of(ISSUE_ATTEMPT);

    public static final int PARTITIONS = 6;

    private static final int REPLICAS_DURABLE = 3;
    private static final int REPLICAS_ATTEMPT = 2;
    private static final String MIN_INSYNC_REPLICAS = "2";

    private static final Duration RETENTION_DURABLE = Duration.ofDays(7);
    private static final Duration RETENTION_ATTEMPT = Duration.ofHours(12);
    /** 격리본은 원본보다 오래 남는다 — 사람이 열어 볼 때까지가 목적이다. */
    private static final Duration RETENTION_DEAD_LETTER = Duration.ofDays(30);
    /** 파티션당이다. 파티션 6개라 토픽 총량은 12GiB. */
    private static final long RETENTION_BYTES_ATTEMPT = 2L * 1024 * 1024 * 1024;

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicConfig.class);

    /**
     * 이 횟수부터 상태를 경보 등급으로 올린다. <b>재시도는 멈추지 않는다</b> — 브로커 롤링
     * 재기동과 배포가 겹치면 그 창이 45초보다 길고, 거기서 끊으면 그 인스턴스는 재기동 전까지
     * 토픽 없이 트래픽을 받는다.
     */
    private static final int PROVISION_ALERT_AFTER_ATTEMPTS = 5;
    private static final Duration PROVISION_INITIAL_BACKOFF = Duration.ofSeconds(3);

    /**
     * 토픽 생성을 기동 경로에서 떼어 낸다.
     *
     * <p><b>실측으로 고친 부분이다.</b> 처음에는 {@code setOperationTimeout(5)} 만 주고
     * "기동 지연 상한 5초" 라고 적었는데 사실이 아니었다 — 어드민 클라이언트 생성 · 토픽 조회 ·
     * 토픽 생성이 각각 상한을 쓰기 때문에 브로커가 없으면 컨텍스트 하나당 약 15초가 걸렸다
     * (테스트 4개에 60초, 결과 XML 13.6MB). {@code operationTimeout} 은 <b>작업 하나의 상한</b>이지
     * {@code initialize()} 전체의 상한이 아니다.
     *
     * <p>그래서 {@code autoCreate} 를 끈다. Spring 이 컨텍스트 refresh 중에 동기로 토픽을
     * 만들지 않고, 아래 {@code kafkaTopicProvisioner} 가 기동을 마친 뒤 별도 스레드에서 한 번
     * 시도한다. 토픽은 그대로 만들어지되 <b>발급 API 기동이 브로커를 기다리지 않는다.</b>
     *
     * <p>어드민 클라이언트 쪽 상한도 함께 조인다. 안 그러면 브로커가 없을 때 재접속 로그가
     * 초 단위로 쌓인다.
     */
    @Bean
    public KafkaAdmin kafkaAdmin(KafkaConnectionProperties properties) {
        KafkaAdmin admin = new KafkaAdmin(Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KafkaProducerSupport.requireBootstrapServers(properties),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000,
                AdminClientConfig.RETRIES_CONFIG, 0,
                AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, 2_000L));
        admin.setOperationTimeout(3);
        admin.setAutoCreate(false);
        // 이미 있는 토픽의 설정(min.insync.replicas · 보존)을 우리 선언에 맞춘다.
        //
        // 기본값 false 면 기존 토픽의 설정은 검사도 수정도 로그도 없다 — addOrModifyTopicsIfNeeded 가
        // 이 플래그를 보고 통째로 건너뛴다(바이트코드로 확인). 브로커가 자동 생성한 토픽은
        // min.insync.replicas 가 브로커 기본값(보통 1)로 남고, 그러면 acks=all 이 "복제본 한 대에만
        // 썼는데 성공" 이 된다. 복제본 수와 달리 설정값은 나중에도 고칠 수 있으니 고치게 둔다.
        admin.setModifyTopicConfigs(true);
        return admin;
    }

    /**
     * 프로비저닝 전용 실행기. 컨텍스트가 소유하므로 종료 시 함께 정리된다.
     *
     * <p>{@code destroyMethod} 를 명시하는 이유 — {@code ExecutorService} 는 Java 19부터
     * {@code AutoCloseable} 이라, 기본 추론에 맡기면 종료 시 {@code close()} 가 불려
     * <b>작업이 끝날 때까지 기다린다.</b> 브로커가 없으면 그만큼 종료가 늦어진다.
     */
    @Bean(destroyMethod = "shutdownNow")
    @ConditionalOnProperty(name = "kafka.provision-topics", matchIfMissing = true)
    public ExecutorService kafkaTopicProvisioningExecutor() {
        return Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("kafka-topic-provisioner").factory());
    }

    /**
     * 기동을 마친 뒤 토픽을 만든다. 실패해도 애플리케이션에 영향을 주지 않는다 —
     * 토픽 선언은 관측·발송 계층의 준비물이고, 이 애플리케이션의 본업은 쿠폰 발급이다.
     *
     * <p>테스트는 {@code kafka.provision-topics=false} 로 끈다 — 선언값을 보는 테스트가
     * 있지도 않은 브로커에 접속을 시도할 이유가 없다.
     */
    @Bean
    @ConditionalOnProperty(name = "kafka.provision-topics", matchIfMissing = true)
    public KafkaTopicProvisioner kafkaTopicProvisioner(
            KafkaAdmin kafkaAdmin,
            @Qualifier("kafkaTopicProvisioningExecutor") ExecutorService executor
    ) {
        return new KafkaTopicProvisioner(provisioningStep(kafkaAdmin::initialize, kafkaAdmin),
                executor, PROVISION_ALERT_AFTER_ATTEMPTS, PROVISION_INITIAL_BACKOFF);
    }

    /**
     * 프로비저닝을 끈 회차는 <b>토픽을 만들지도 확인하지도 않는다.</b> 지표는 N_A 를 내는데 그건
     * 정상값처럼 읽히므로, 기동에 한 줄 남겨 그 회차가 조용하지 않게 한다.
     *
     * <p>검증 전용 스위치를 따로 만들지 않는 이유 — 스위치가 둘이 되면 "만들되 확인하지 않는다"
     * 라는 조합이 생기고, 그건 이 계층이 없애기로 한 상태다(지표가 거짓말하는 상태).
     */
    @Bean
    @ConditionalOnProperty(name = "kafka.provision-topics", havingValue = "false")
    public InitializingBean kafkaTopicProvisioningDisabledNotice() {
        return () -> log.warn("kafka.provision-topics=false — 토픽 생성도 선언 검증도 하지 않는다."
                + " 브로커의 auto.create.topics.enable 이 꺼져 있는지 배포 전에 확인할 것");
    }

    /**
     * 프로비저닝 지표. <b>프로비저너가 없어도 등록된다</b> — 조건이 {@code kafka.enabled} 뿐이다.
     *
     * <p>지표를 프로비저너 빈 안에서 등록하면 {@code kafka.provision-topics=false} 인 인스턴스는
     * 시계열을 통째로 안 내보낸다. 그런데 그 설정이 정확히 <b>가장 위험한 조합</b>이다 — 토픽을
     * 아무도 안 만든 채 발급이 시작되면 브로커가 RF1 짜리를 대신 만든다. 운영 지침이 가리키는
     * 지표가 하필 그 회차에서만 사라지면 안 된다.
     *
     * <p>값과 상태를 짝으로 낸다. 값은 확인됐을 때만 1 이고 나머지는 NaN 이며, 이유는 상태
     * 미터가 {@link SourceStatusCode} 로 낸다.
     */
    @Bean
    public MeterBinder kafkaTopicProvisioningMeters(ObjectProvider<KafkaTopicProvisioner> provisioners) {
        return registry -> {
            Gauge.builder(DomainMeterNames.KAFKA_TOPICS_PROVISIONED, provisioners,
                            available -> value(available.getIfAvailable()))
                    .description("토픽 선언이 브로커에 반영됐는지 (확인 전에는 값 없음)")
                    .register(registry);
            Gauge.builder(DomainMeterNames.KAFKA_TOPICS_PROVISIONED_STATE, provisioners,
                            available -> SourceStatusCode.of(snapshot(available.getIfAvailable()).status()))
                    .description("토픽 반영 여부를 확인하지 못한 이유")
                    .register(registry);
            for (String cause : PROVISION_CAUSES) {
                Gauge.builder(DomainMeterNames.KAFKA_TOPICS_PROVISIONED_CAUSE, provisioners,
                                available -> cause.equals(snapshot(available.getIfAvailable()).cause()) ? 1 : 0)
                        .tag(DomainMeterNames.TAG_CAUSE, cause)
                        .description("확인 실패의 종류 (현재 원인만 1)")
                        .register(registry);
            }
        };
    }

    /**
     * 값 판정은 <b>프로비저너가 소유한다.</b> 여기서 {@code carriesValue()} 로 따로 판정하면
     * "반영됨" 의 정의가 둘이 된다 — 그 술어는 "값을 실어야 하는 상태인가" 이지 "브로커에
     * 반영됐는가" 가 아니라서, {@code STALE} 같은 값이 들어오는 순간 게이지는 1(반영됨)을,
     * {@code isProvisioned()} 는 false 를 낸다.
     */
    private static double value(KafkaTopicProvisioner provisioner) {
        return provisioner == null ? Double.NaN : provisioner.provisionedValue();
    }

    /**
     * 상태와 원인을 한 번에 읽는다 — 둘을 따로 읽으면 한 scrape 안에서 어긋난다.
     *
     * <p>프로비저너가 없다는 것은 이 회차가 프로비저닝을 하지 않기로 했다는 뜻이다 — 실패가 아니다.
     */
    private static KafkaTopicProvisioner.Snapshot snapshot(KafkaTopicProvisioner provisioner) {
        return provisioner == null
                ? new KafkaTopicProvisioner.Snapshot(SourceStatus.N_A, "none")
                : provisioner.snapshot();
    }

    /** 꼬리표 값은 등록 시점에 고정된다 — 그래서 닫힌 4값을 각각 한 시계열로 낸다. */
    private static final List<String> PROVISION_CAUSES = List.of("none", "unconfirmed", "mismatched", "shutdown");

    /**
     * 생성과 확인을 한 걸음으로 묶는다. <b>람다로 두면 이 결합에 가드를 걸 수 없다</b> —
     * {@code && matchesDeclaration(...)} 한 조각만 지워도 검증 메서드의 테스트는 전부 초록이라
     * 아무도 모르게 예전 상태로 돌아간다.
     *
     * <p>{@code create} 를 따로 받는 이유는 {@code KafkaAdmin.initialize()} 가 {@code final} 이라
     * 스텁할 수 없기 때문이다. 단락 평가도 계약이다 — 만들지 못했으면 확인하러 가지 않는다.
     */
    static Supplier<ProvisionOutcome> provisioningStep(BooleanSupplier create, KafkaAdmin admin) {
        // 인자로 넘기면 즉시 평가되어 빈 생성(= refresh) 중에 브로커를 조회한다. 시도마다 읽는다.
        return () -> {
            if (!create.getAsBoolean()) {
                return ProvisionOutcome.UNCONFIRMED;
            }
            return verifyDeclaration(admin, brokerConfigs(admin));
        };
    }

    static Supplier<ProvisionOutcome> provisioningStep(
            BooleanSupplier create, KafkaAdmin admin, UnaryOperator<String> floorOf) {
        return () -> {
            if (!create.getAsBoolean()) {
                // 만들지 못했다. 브로커가 안 떴을 수도, 복제본을 만족 못 했을 수도 있다 — 둘 다 재시도 대상이다.
                return ProvisionOutcome.UNCONFIRMED;
            }
            return verifyDeclaration(admin, floorOf);
        };
    }

    /**
     * <b>{@code initialize()} 의 true 를 그대로 믿지 않는다.</b>
     *
     * <p>Spring 의 어드민은 이미 있는 토픽에 대해 파티션 수와 설정만 맞춘다 — <b>복제본 수는
     * 비교조차 하지 않는다</b>(클래스 바이트코드 전체에 {@code replic} 문자열이 없다). 그래서
     * 브로커가 RF1 로 자동 생성해 둔 토픽에도 {@code initialize()} 는 true 를 돌려준다.
     *
     * <p>그 상태가 정확히 이 지표가 잡으려던 사고다. RF 는 만들 때만 정해지고 나중에 바꾸려면
     * 파티션 재배치라는 별도 운영 작업이 필요하므로, <b>고치지 않고 크게 알린다.</b>
     *
     * <p>브로커에 닿지 못하면 예외가 나고 호출부가 재시도한다 — 그건 불일치가 아니라 미확인이다.
     * 응답에서 토픽이 빠진 경우도 같다. <b>재시도가 고칠 수 있는 실패와 그렇지 않은 실패를 값으로
     * 가른다</b> — boolean 하나면 그 둘이 뭉쳐서, 재기동하면 나을 상황에 "재배치하라" 는 지시가 나간다.
     */
    static ProvisionOutcome verifyDeclaration(KafkaAdmin admin) {
        return verifyDeclaration(admin, brokerConfigs(admin));
    }

    static ProvisionOutcome verifyDeclaration(KafkaAdmin admin, UnaryOperator<String> floorOf) {
        Map<String, TopicDescription> described;
        try {
            described = admin.describeTopics(allTopics().toArray(String[]::new));
        } catch (KafkaException failure) {
            if (!hasCause(failure, UnknownTopicOrPartitionException.class)) {
                throw failure;
            }
            // 아직 없는 토픽이 있다. 스택트레이스만 남기면 "어느 토픽이" 가 사라진다.
            log.warn("브로커에 아직 없는 토픽이 있다 — 다시 시도한다. 선언: {}", allTopics());
            return ProvisionOutcome.UNCONFIRMED;
        }
        if (!described.keySet().containsAll(allTopics())) {
            // 응답이 비었다고 "확인했다" 로 넘어가면 공허하다. 다만 이건 <b>불일치가 아니라 미확인</b>이다 —
            // 다른 인스턴스가 만드는 중일 수도 있으므로 재시도가 고칠 수 있다.
            log.warn("브로커가 답하지 않은 토픽이 있다 — 다시 시도한다: {}", allTopics().stream()
                    .filter(topic -> !described.containsKey(topic))
                    .toList());
            return ProvisionOutcome.UNCONFIRMED;
        }
        boolean matches = true;
        for (Map.Entry<String, TopicDescription> entry : described.entrySet()) {
            matches &= matches(entry.getKey(), entry.getValue());
        }
        if (!matches) {
            return ProvisionOutcome.MISMATCHED;
        }
        // 파티션·복제본이 맞아도 ISR 하한이 낮으면 acks=all 의 약속이 깨진다.
        return verifyInSyncFloor(floorOf);
    }

    /**
     * 브로커에 <b>지금 걸려 있는</b> ISR 하한을 선언과 대조한다.
     *
     * <p>파티션·복제본만 보면 만들어진 뒤 누가 {@code min.insync.replicas} 를 1 로 낮춰도
     * 지표는 반영됨(1)이다. 그 구간의 {@code acks=all} 은 복제본 한 대에만 쓰고 성공한다 —
     * 이 계층이 막으려던 "영구 미영속 발급" 이 정확히 거기서 생긴다.
     *
     * <p>설정은 다음 {@code initialize()} 가 되돌릴 수 있으므로 불일치가 아니라 미확인이다.
     *
     * @param floorOf 토픽 이름을 받아 현재 걸린 ISR 하한을 주는 함수. 읽지 못했으면 {@code null}
     */
    static ProvisionOutcome verifyInSyncFloor(UnaryOperator<String> floorOf) {
        for (String topic : allTopics()) {
            if (ISSUE_ATTEMPT.equals(topic)) {
                continue;   // acks=0 이라 ISR 하한을 선언하지 않는다
            }
            String floor = floorOf.apply(topic);
            if (!MIN_INSYNC_REPLICAS.equals(floor)) {
                log.error("{} 의 min.insync.replicas 가 선언({})과 다르다: {} —"
                        + " acks=all 이 복제본 한 대에만 쓰고 성공할 수 있다",
                        topic, MIN_INSYNC_REPLICAS, floor);
                return ProvisionOutcome.UNCONFIRMED;
            }
        }
        return ProvisionOutcome.PROVISIONED;
    }

    /**
     * 어드민 클라이언트 생성 지점. <b>테스트가 "기동 중에 만들어졌는가" 를 세기 위한 이음새다</b> —
     * 시간으로 재면 DNS·타임아웃·CI 부하에 따라 결함이 그대로인데 초록이 뜬다.
     */
    static final Function<Map<String, Object>, Admin> DEFAULT_ADMIN_FACTORY = Admin::create;

    static Function<Map<String, Object>, Admin> adminFactory = DEFAULT_ADMIN_FACTORY;

    /** {@code KafkaAdmin} 은 {@code describeConfigs} 를 열어 주지 않아 어드민을 직접 연다. */
    private static UnaryOperator<String> brokerConfigs(KafkaAdmin admin) {
        Map<String, String> floors = new HashMap<>();
        try (Admin client = adminFactory.apply(admin.getConfigurationProperties())) {
            List<ConfigResource> resources = allTopics().stream()
                    .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                    .toList();
            client.describeConfigs(resources).all().get(3, TimeUnit.SECONDS)
                    .forEach((resource, config) -> {
                        ConfigEntry entry = config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
                        if (entry != null) {
                            floors.put(resource.name(), entry.value());
                        }
                    });
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return topic -> null;
        } catch (Exception failure) {
            log.warn("토픽 설정을 읽지 못했다 — 다시 시도한다", failure);
            return topic -> null;
        }
        return floors::get;
    }

    /** 순환 참조하는 예외 체인에서 빠져나오지 못하면 그 스레드가 영영 소모된다. */
    private static final int MAX_CAUSE_DEPTH = 16;

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
        // 깊이 상한이 사이클 추적보다 싸고 확실하다. 자기참조만 막으면 A→B→A 를 못 빠져나온다.
        int depth = 0;
        for (Throwable cause = failure; cause != null && depth++ < MAX_CAUSE_DEPTH; cause = cause.getCause()) {
            if (type.isInstance(cause)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matches(String topic, TopicDescription described) {
        int expectedReplicas = ISSUE_ATTEMPT.equals(topic) ? REPLICAS_ATTEMPT : REPLICAS_DURABLE;
        int partitions = described.partitions().size();
        int replicas = described.partitions().stream()
                .mapToInt(partition -> partition.replicas().size())
                .min()
                .orElse(0);
        if (partitions == PARTITIONS && replicas >= expectedReplicas) {
            return true;
        }
        log.error("토픽 {} 이 선언과 다르다 — 파티션 {}(기대 {}) · 복제본 {}(기대 {})."
                        + " 복제본 수는 만들 때만 정해지고 재배치 없이는 못 고친다."
                        + " 브로커가 자동 생성한 토픽이면 그대로 두면 안 된다",
                topic, partitions, PARTITIONS, replicas, expectedReplicas);
        return false;
    }

    @Bean
    public NewTopic issuePersistTopic() {
        return durable(ISSUE_PERSIST);
    }

    @Bean
    public NewTopic issuePersistDltTopic() {
        return deadLetter(ISSUE_PERSIST_DLT);
    }

    /**
     * 유일하게 RF 2 다. 유실을 감수하는 토픽이라(acks=0) 복제본 하나를 더 두는 값이
     * 지켜 줄 것이 없다. 보존도 유일하게 용량 상한을 함께 건다.
     */
    @Bean
    public NewTopic issueAttemptTopic() {
        return TopicBuilder.name(ISSUE_ATTEMPT)
                .partitions(PARTITIONS)
                .replicas(REPLICAS_ATTEMPT)
                .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(RETENTION_ATTEMPT.toMillis()))
                .config(TopicConfig.RETENTION_BYTES_CONFIG, Long.toString(RETENTION_BYTES_ATTEMPT))
                .build();
    }

    @Bean
    public NewTopic notifyTopic() {
        return durable(NOTIFY);
    }

    @Bean
    public NewTopic notifyDltTopic() {
        return deadLetter(NOTIFY_DLT);
    }

    /** 선언된 토픽 전체. 계약 테스트가 목록을 자동으로 얻게 둔다 — 손으로 옮겨 적으면 어긋난다. */
    public static List<String> allTopics() {
        return List.of(ISSUE_PERSIST, ISSUE_ATTEMPT, NOTIFY, ISSUE_PERSIST_DLT, NOTIFY_DLT);
    }

    /**
     * 격리본은 <b>사람이 볼 때까지</b> 남아 있어야 한다. 원본과 같은 보존을 주면 원본이 만료될 때
     * 증거도 같이 사라져서, DLT 에 넣은 것이 그냥 둔 것보다 나은 점이 없어진다. 금요일 밤에 쌓인
     * 격리본을 다음 주에 열어 보는 상황을 견디는 값이다.
     */
    private static NewTopic deadLetter(String name) {
        return TopicBuilder.name(name)
                .partitions(PARTITIONS)
                .replicas(REPLICAS_DURABLE)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(RETENTION_DEAD_LETTER.toMillis()))
                .build();
    }

    private static NewTopic durable(String name) {
        return TopicBuilder.name(name)
                .partitions(PARTITIONS)
                .replicas(REPLICAS_DURABLE)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(RETENTION_DURABLE.toMillis()))
                .build();
    }
}
