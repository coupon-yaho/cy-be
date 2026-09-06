package com.kafkick.infra.mq.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.springframework.util.Assert;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * 컨슈머 그룹과 {@code auto.offset.reset} 을 못박는다. 컨슈머 구현은 OBS-15 지만,
 * 이 두 값은 <b>정책</b>이라 설정 계층이 소유한다.
 *
 * <pre>
 * coupon-persist    earliest   한 건도 놓치면 '영구 미영속 발급' = 위반
 * attempt-live      latest     화면은 지금을 본다
 * attempt-archive   latest     재기동 시 수십만 건을 따라잡으면 DB 를 때린다
 * notify-dispatch   earliest   발송 누락이 사용자에게 보인다
 * </pre>
 *
 * <p><b>양쪽을 다 {@code earliest} 로 두면 안 된다.</b> attempt 는 초당 수천 건이 쌓이는
 * 관측 로그다. 컨슈머가 잠깐 내려갔다 올라올 때마다 밀린 전량을 DB 로 밀어 넣으면,
 * 관측이 측정 대상을 죽인다.
 *
 * <p>attempt 쪽 그룹이 둘인 것도 계약이다 — {@code attempt-live} 와 {@code attempt-archive} 는
 * 서로 다른 {@code group.id} 여야 각자 전체 메시지를 받는다. 같은 그룹을 쓰면 파티션을 나눠
 * 가져 화면과 적재가 서로 절반씩만 본다.
 *
 * <h2>상수만 두면 지켜지지 않는다</h2>
 *
 * 그래서 {@link #consumerConfig(String, String)} 를 함께 둔다. OBS-15 가 이 메서드를 통과하면
 * 그룹별 offset 정책이 자동으로 붙고, <b>정책이 없는 그룹은 그 자리에서 거부된다.</b>
 * 리스너에 {@code groupId} 를 문자열로 직접 적고 {@code auto.offset.reset} 을 기본값(latest)에
 * 맡기면 persist 가 조용히 latest 가 되는데, 그건 이 클래스가 "위반" 이라고 적어 둔 상태다.
 * 그 갈라짐이 리뷰에서 눈에 띄게 하려면 소비 지점이 있어야 한다.
 */
public final class KafkaConsumerGroups {

    public static final String PERSIST = "coupon-persist";
    public static final String ATTEMPT_LIVE = "attempt-live";
    public static final String ATTEMPT_ARCHIVE = "attempt-archive";
    public static final String NOTIFY_DISPATCH = "notify-dispatch";

    /**
     * DLT 재처리(batch 설계 8번). <b>earliest 다</b> — 격리본을 처음부터 다시 읽는 것이 목적이라
     * latest 면 이미 쌓인 것을 통째로 건너뛰어 아무것도 재처리하지 않는다.
     *
     * <p>배치가 아직 없는데 지금 등록하는 이유 — 등록되지 않은 그룹은
     * {@link #consumerConfig(String, String)} 이 거부한다. 그러면 만드는 사람이 이 문을 안 거치고
     * 직접 설정 맵을 쓰게 되고, 그 순간 기본값 latest 가 조용히 들어온다.
     */
    public static final String DLT_REPROCESS = "coupon-dlt-reprocess";

    /**
     * 한 번의 {@code poll} 이 가져오는 최대 레코드 수. <b>Kafka 기본값과 같은 500 이지만
     * 못박는다</b> — 이 값이 {@link #MAX_POLL_INTERVAL_MILLIS} 와 짝을 이뤄 <b>한 묶음을 처리할
     * 시간 예산</b>을 정하기 때문이다. 기본값에 맡기면 클라이언트 판이 올라가며 조용히 바뀔 수
     * 있고, 그 순간 그 예산에서 유도한 상한이 근거를 잃는다.
     *
     * <p>이 둘을 바꾸면 {@code HttpNotificationSender} 의 건당 상한이 함께 바뀐다 —
     * 그쪽이 여기서 계산해 가므로 <b>따로 고칠 것은 없다.</b>
     */
    public static final long MAX_POLL_RECORDS = 500;

    /**
     * 한 {@code poll} 묶음을 처리할 수 있는 시간(기본값과 같은 5분, 못박는다).
     *
     * <p>넘기면 소비자가 그룹에서 <b>쫓겨나고 그 묶음이 통째로 재전달된다</b> — 이미 처리한
     * 것까지 다시 온다. 알림은 그것이 곧 중복 발송이다.
     */
    public static final long MAX_POLL_INTERVAL_MILLIS = 300_000;

    public static final String EARLIEST = "earliest";
    public static final String LATEST = "latest";

    private static final Map<String, String> OFFSET_RESETS = Map.of(
            PERSIST, EARLIEST,
            ATTEMPT_LIVE, LATEST,
            ATTEMPT_ARCHIVE, LATEST,
            NOTIFY_DISPATCH, EARLIEST,
            DLT_REPROCESS, EARLIEST);

    private KafkaConsumerGroups() {}

    public static String offsetResetOf(String groupId) {
        String reset = OFFSET_RESETS.get(groupId);
        if (reset == null) {
            throw new IllegalArgumentException("offset 정책이 정해지지 않은 그룹이다: " + groupId);
        }
        return reset;
    }

    public static Map<String, String> offsetResets() {
        return OFFSET_RESETS;
    }

    /**
     * 컨슈머 공통 설정. OBS-15 의 {@code ConsumerFactory} 가 여기서 출발한다.
     *
     * <p>{@code allow.auto.create.topics} 를 끄는 이유 — 기본값이 {@code true} 라 토픽 이름에
     * 오타가 나면 <b>그 이름의 토픽이 새로 생긴다.</b> 아무 메시지도 안 오는데 에러도 안 난다.
     *
     * <p>{@code enable.auto.commit} 은 Kafka 4 에서 기본값이 이미 {@code false} 지만 명시한다.
     * 3.x 는 {@code true} 였어서, 예제를 옮겨 온 사람이 "기본이 자동 커밋" 이라고 읽는 순간
     * offset 관리 주체가 헷갈린다. 커밋은 Spring 리스너 컨테이너가 한다.
     */
    public static Map<String, Object> consumerConfig(String groupId, String bootstrapServers) {
        // 프로듀서 쪽과 같은 문이다. 빈 값을 통과시키면 리스너가 브로커를 못 찾은 채 조용히 돈다.
        Assert.hasText(bootstrapServers, "kafka.bootstrap-servers 가 비어 있다. 컨슈머 설정을 만들 수 없다.");
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetOf(groupId));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        // 기본값과 같은 값을 굳이 적는다 — 위 두 상수 주석 참고. 처리 시간 예산의 분모·분자다.
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, (int) MAX_POLL_RECORDS);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) MAX_POLL_INTERVAL_MILLIS);
        return config;
    }
}
