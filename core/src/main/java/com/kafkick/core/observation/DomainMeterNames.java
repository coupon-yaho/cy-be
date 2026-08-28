package com.kafkick.core.observation;

import com.kafkick.core.consistency.ConsistencyGapType;

/**
 * batch·infra:mq 가 등록하고 조회 API(OBS-6)가 읽는 도메인 Gauge 이름. 양쪽이 문자열을 각자 옮겨 적으면
 * 한쪽만 바뀌어도 예외 없이 "값 없음" 만 오고 앱은 정상 기동한다 — 로그도 없이 화면만 빈다.
 *
 * <p>읽는 쪽이 api 면 이름은 여기다 — 등록 주체가 batch 든 infra:mq 든 상관없다. api 는 그
 * 모듈들을 {@code runtimeOnly} 로만 의존해서 저쪽 상수를 컴파일 타임에 볼 수 없기 때문이다.
 * {@code api/observation/MeterNames} 는 api 가 직접 만드는 HTTP 미터의 자리다.
 *
 * <p><b>값과 상태를 항상 짝으로 낸다.</b> Prometheus 샘플은 숫자 하나뿐이라 "값 없음" 의 이유
 * (N_A · UNAVAILABLE · PENDING)를 담을 자리가 없다. 값이 없을 때 0 을 실으면 "정상인데 0" 과
 * 구분되지 않으므로, 값 미터는 NaN 을 싣고 이유는 상태 미터가 {@link SourceStatusCode} 로 낸다.
 *
 * <p><b>회차 식별자는 라벨이 아니다.</b> 라벨 하나가 시계열 집합을 그 값 개수만큼 곱하고,
 * 회차마다 시계열이 통째로 갈리면 회차 간 비교 질의를 쓸 수 없다. 관측 대상 회차는
 * {@link #OBSERVED_COUPON_ID} 의 <b>값</b>으로 싣고, 회차 경계는 질의 시간 범위로 자른다.
 */
public final class DomainMeterNames {

    /** 정합성 gap 4종. {@link #TAG_GAP_TYPE} 로만 나뉘어 시계열은 종류당 하나다. */
    public static final String CONSISTENCY_GAP = "app.consistency.gap";
    public static final String CONSISTENCY_GAP_STATE = "app.consistency.gap.state";

    /** gap 과 별개인 독립 KPI. 0 보다 크면 팔지 않은 재고를 판 것이다. */
    public static final String OVER_ISSUED = "app.consistency.over.issued";
    public static final String OVER_ISSUED_STATE = "app.consistency.over.issued.state";

    public static final String QUEUE_LENGTH = "app.queue.length";
    public static final String QUEUE_LENGTH_STATE = "app.queue.length.state";

    public static final String STOCK_REMAINING = "app.coupon.stock.remaining";
    public static final String STOCK_REMAINING_STATE = "app.coupon.stock.remaining.state";

    /** v2 issued Hash의 회차별 노후 PENDING 수와 수집 상태. */
    public static final String STALE_PENDING_COUNT = "app.coupon.v2.issued.stale.pending";
    public static final String STALE_PENDING_COUNT_STATE = "app.coupon.v2.issued.stale.pending.state";

    /** v2 issued Hash의 회차별 codec 파손 field 수와 수집 상태. */
    public static final String CORRUPT_FIELD_COUNT = "app.coupon.v2.issued.corrupt.fields";
    public static final String CORRUPT_FIELD_COUNT_STATE = "app.coupon.v2.issued.corrupt.fields.state";

    /** PENDING 계측만 회차별 과거 조회가 필요해 쓰는 제한적 라벨. */
    public static final String TAG_COUPON_ROUND_ID = "couponRoundId";

    /** FINAL 진입 게이트인 quiet period 를 재는 기준 시각(epoch 초). */
    public static final String LAST_SUCCESSFUL_ISSUE_EPOCH = "app.issuance.last.success.epoch";
    public static final String LAST_SUCCESSFUL_ISSUE_EPOCH_STATE = "app.issuance.last.success.epoch.state";

    /** 재고·대기열 미터가 어느 회차를 본 값인지. 라벨이 아니라 값이라 시계열이 늘지 않는다. */
    public static final String OBSERVED_COUPON_ID = "app.observation.coupon.id";

    /**
     * 정합성 미터가 어느 회차를 본 값인지. {@link #OBSERVED_COUPON_ID} 와 <b>따로 낸다</b> —
     * 재고는 1초, 정합성은 그보다 느린 주기로 갱신되므로 회차가 바뀌는 순간 둘이 잠시 어긋난다.
     * 하나로 합치면 그 창에서 이전 회차의 gap 이 새 회차 값으로 읽힌다. 화면과 질의는 두 값이
     * 같은 구간만 회차 단위로 비교해야 한다.
     */
    public static final String CONSISTENCY_COUPON_ID = "app.consistency.coupon.id";

    /**
     * 수집 경로별 <b>마지막 성공 시각</b>(epoch 초). 한 번도 성공하지 못했으면 NaN 이다.
     *
     * <p>상태 미터만으로는 "잠깐 끊겼다" 와 "5분째 같은 이유로 잘린다" 가 구분되지 않는다.
     * 후자는 gap 이 영원히 UNAVAILABLE 로 굳은 상태라, 화면은 조용한데 초과 발급 KPI 가 죽는다.
     *
     * <p><b>횟수가 아니라 시각인 이유</b> — 수집 루프는 주기가 서로 다르다(재고 1초 · 정합성 30초).
     * "연속 3회 실패" 가 한쪽은 3초, 다른 쪽은 90초라 같은 임계값을 쓸 수 없고, 주기를 조정할
     * 때마다 경보를 함께 고쳐야 한다. 시각으로 내면 {@code time() - 값 > 120} 하나로 두 경로를
     * 같은 규칙으로 본다.
     */
    public static final String COLLECT_LAST_SUCCESS_EPOCH = "app.observation.collect.last.success.epoch";

    /**
     * 정합성 원시값을 읽은 두 원천의 관측 시각 차이(초). Redis 를 먼저 읽고 DB 집계를 뒤에 돌리므로
     * 항상 0 이상이며, Redis 쪽이 그만큼 더 과거다.
     *
     * <p>두 원천을 같은 순간에 얼릴 방법은 없다. 그래서 창을 없애는 대신 <b>크기를 함께 실어</b>
     * 읽는 쪽이 "이 gap 중 최대 얼마가 시차 탓인지" 를 판단할 수 있게 한다. 이 값이 크게 튀는
     * 구간의 gap 은 불일치가 아니라 집계 지연일 수 있다.
     */
    public static final String CONSISTENCY_SOURCE_SKEW_SECONDS = "app.consistency.source.skew.seconds";

    // TODO(OBS-17 후속 티켓, @rudwnlee2): Kafka persist lag 미터가 여기 하나 더 붙는다. 관측-Batch
    //   포트 계약은 batch 산출물을 4종(정합성 gap · 대기열 · 재고 · Kafka persist lag)으로 적는데
    //   지금은 앞의 3종뿐이다 — lag 은 AdminClient 배선이 필요하고, OBS-17 은 프로듀서까지만 세웠다.
    //   그 통로가 열리는 시점은 BatchKafkaLagAssumptionTest 가 알려 준다.

    // ── Kafka 계층(OBS-17). 등록은 infra:mq 이고 읽는 쪽은 api 라 여기 둔다 ──────────────
    //
    // api 는 infra:mq 를 runtimeOnly 로만 의존해서 그 모듈의 상수를 컴파일 타임에 볼 수 없다.
    // 이름을 저쪽에 두면 조회하는 쪽이 문자열을 옮겨 적는 것 말고는 방법이 없다.

    /**
     * 발급 경로에서 <b>삼킨</b> attempt 이벤트 발행 실패 수. {@link #TAG_REASON} 으로만 나뉜다.
     *
     * <p>0 이 아니면 화면의 attempt 수치가 이미 비어 있다는 뜻이다. 다만 이 값으로 TPS·성공률을
     * 보정하면 안 된다 — attempt 는 판정 원천이 아니다.
     */
    public static final String KAFKA_ATTEMPT_PUBLISH_FAILURES = "app.kafka.attempt.publish.failures";

    /**
     * 토픽 선언이 브로커에 반영됐는지. 반영 확인 전에는 값이 없고(NaN) 이유는 상태 미터가 낸다.
     *
     * <p>확인되지 않은 채로 발급이 시작되면 브로커가 토픽을 대신 만든다
     * ({@code auto.create.topics.enable} 기본값 true, RF 1). 그러면 RF3·ISR2·파티션6 계약이
     * 무효가 되고 <b>RF 는 되돌릴 수 없다</b>.
     */
    public static final String KAFKA_TOPICS_PROVISIONED = "app.kafka.topics.provisioned";

    /**
     * 위 값이 없는 이유. {@code N_A}=프로비저닝을 끈 회차 · {@code PENDING}=아직 확인 전 ·
     * {@code UNAVAILABLE}=확인하지 못했다({@link #KAFKA_TOPICS_PROVISIONED_CAUSE} 가 원인을 가른다) · {@code VALID}=반영됨.
     */
    public static final String KAFKA_TOPICS_PROVISIONED_STATE = "app.kafka.topics.provisioned.state";

    /**
     * 확인 실패의 <b>종류</b>. {@link #TAG_CAUSE} 가 {@code none · unconfirmed · mismatched}
     * 셋으로 닫혀 있고, 현재 원인만 1 이다.
     *
     * <p>상태 미터의 {@code UNAVAILABLE} 하나로는 "브로커가 아직 안 떴다"(재기동·대기로 낫는다)와
     * "선언과 다른 토픽이 이미 있다"(토픽을 다시 만들어야 낫는다)가 같은 값으로 보인다.
     */
    public static final String KAFKA_TOPICS_PROVISIONED_CAUSE = "app.kafka.topics.provisioned.cause";

    // ── attempt 컨슈머 계층(OBS-15). 등록은 infra:mq 이고 읽는 쪽은 api 라 여기 둔다 ──────
    //
    // 위 Kafka 계층과 같은 이유다. 이름을 컨슈머 옆에 두면 조회하는 쪽이 문자열을 옮겨 적는다.

    /**
     * live 층화 샘플링의 <b>판정</b> 수. {@link #TAG_SAMPLING_DECISION} 이
     * {@code admitted · dropped} 둘로 닫혀 있다.
     *
     * <p>이 값이 필요한 이유는 {@code max-per-second} 가 하드 상한이 아니기 때문이다 —
     * 층별 최소 보장이 그것을 넘길 수 있어서, 설정만 읽으면 실제 Redis 쓰기량을 모른다.
     * 값 검증을 사람이 아니라 지표가 한다.
     *
     * <p>합(admitted + dropped)은 컨슈머가 <b>받은</b> 건수이지 발급 경로가 <b>보낸</b> 건수가
     * 아니다. attempt 토픽은 {@code acks=0} 이라 그 둘이 다르다.
     */
    public static final String ATTEMPT_LIVE_SAMPLED = "app.attempt.live.sampled";

    /**
     * live 버퍼 쓰기에서 <b>삼킨</b> 실패 수.
     *
     * <p>0 이 아니면 화면이 비어 있는데 Kafka 소비는 정상이라는 뜻이다. 이 실패는 offset 을
     * 막지 않는다 — 화면 하나 때문에 컨슈머 그룹이 멈추면 같은 토픽을 읽는 archive 까지
     * 리밸런싱에 휘말린다. 대신 여기서 소리를 낸다.
     */
    public static final String ATTEMPT_LIVE_APPEND_FAILURES = "app.attempt.live.append.failures";

    /**
     * live 버퍼에서 <b>읽다가</b> 풀지 못해 건너뛴 항목 수.
     *
     * <p>쓰기 실패({@link #ATTEMPT_LIVE_APPEND_FAILURES})와 짝이다. 그쪽이 0 인데 이쪽이 오르면
     * 원인은 Redis 가 아니라 <b>형식</b>이다 — 배포 경계를 사이에 두고 두 형식이 버퍼에 함께
     * 앉아 있다는 뜻이다.
     *
     * <p>이 값이 없으면 그 상황이 어떤 신호도 내지 않는다. 커서는 정상이고 트림도 없어서
     * {@code cursorExpired} 도 거짓이라, 화면은 그냥 항목이 적을 뿐이고 운영자는 그것을
     * 정상으로 읽는다.
     */
    public static final String ATTEMPT_LIVE_UNREADABLE = "app.attempt.live.unreadable";

    /**
     * archive 적재 결과. {@link #TAG_ARCHIVE_OUTCOME} 이 {@code inserted · duplicate} 둘로 닫혀 있다.
     *
     * <p>{@code duplicate} 는 <b>정상값이다</b> — 리밸런싱 후 재소비가 정상 경로이므로 그
     * 직후에 튄다. 다만 리밸런싱과 무관한 구간에서 계속 오르면 키 설계를 의심한다.
     */
    public static final String ATTEMPT_ARCHIVE_OUTCOME = "app.attempt.archive.outcome";

    /**
     * 계약을 위반해 <b>격리하고 offset 을 넘긴</b> 레코드 수. {@link #TAG_REASON} 으로 나뉜다.
     *
     * <p>값은 {@code unknown_enum}(모르는 enum 값) · {@code unsupported_schema}(지원하지 않는
     * {@code schemaVersion}) · {@code other} 셋으로 닫혀 있다.
     *
     * <p>격리만 하고 offset 을 안 넘기면 같은 레코드에서 컨슈머가 무한 재시도한다(poison
     * message). 넘기는 대신 여기서 세므로, <b>이 값이 0 이 아니면 무언가를 버렸다</b>는 뜻이다.
     */
    public static final String ATTEMPT_CONTRACT_VIOLATIONS = "app.attempt.contract.violations";

    /**
     * batch 가 믿고 있는 발급 엔진 버전(V1=1 · V2=2 · V3=3).
     *
     * <p>batch 는 기동 시점의 설정으로 이 값을 정한다. 런타임 전환(OBS-19)이 열리면 관리자가
     * 바꾼 값과 어긋날 수 있는데, 그때 Redis 계열 gap 은 "해당 없음" 으로 나가 <b>없는 것처럼</b>
     * 보인다. 값이 비는 게 아니라서 absent() 로도 안 잡힌다 — 그래서 무엇을 믿고 있는지를 낸다.
     */
    public static final String ENGINE_VERSION = "app.observation.engine.version";

    /**
     * 계산기가 낸 LIVE 심각도(NONE=0 · WARN=1 · CRITICAL=2). 계산할 수 있는 값이 하나도 없으면
     * NaN 이고 이유는 짝이 되는 상태 미터가 낸다.
     *
     * <p>임계치는 {@code observation.consistency.severity.*} 가 정하고 판정은 계산기가 한다.
     * 이 값을 안 내보내면 화면이 같은 임계치를 <b>다시 구현</b>하게 되고, 그 순간 화면의 경보와
     * 검증 배치의 판정이 서로 다른 기준을 쓴다.
     */
    public static final String CONSISTENCY_SEVERITY = "app.consistency.severity";
    public static final String CONSISTENCY_SEVERITY_STATE = "app.consistency.severity.state";

    /** {@link #CONSISTENCY_SOURCE_SKEW_SECONDS} 의 상태 짝. 못 잰 것과 0 을 구분한다. */
    public static final String CONSISTENCY_SOURCE_SKEW_SECONDS_STATE =
        "app.consistency.source.skew.seconds.state";

    public static final String TAG_GAP_TYPE = "type";

    /**
     * 평가 단계 라벨. 이 미터들은 부하 중 추세를 보는 LIVE 값이고, 합격/불합격을 가르는 FINAL
     * 판정은 조용해진 뒤 검증 배치가 따로 한다. 라벨이 없으면 읽는 쪽이 둘을 같은 것으로 읽는다.
     */
    public static final String TAG_PHASE = "phase";

    public static final String PHASE_LIVE = "live";

    /** attempt 발행 실패의 원인. 값은 닫힌 집합이라 예외 종류가 늘어도 시계열이 안 늘어난다. */
    public static final String TAG_REASON = "reason";

    /**
     * 확인 실패의 종류. 값은 {@code none · unconfirmed · mismatched · shutdown} 넷으로 닫혀 있다.
     * {@code shutdown} 은 브로커 문제가 아니라 <b>우리가 종료 중이라 그만둔 것</b>이다.
     */
    public static final String TAG_CAUSE = "cause";

    /** 미터가 가리키는 토픽 이름. */
    public static final String TAG_TOPIC = "topic";

    /** 층화 샘플링 판정. 값은 {@code admitted · dropped} 둘로 닫혀 있다. */
    public static final String TAG_SAMPLING_DECISION = "decision";

    public static final String SAMPLING_ADMITTED = "admitted";
    public static final String SAMPLING_DROPPED = "dropped";

    /** archive 적재 결과. 값은 {@code inserted · duplicate} 둘로 닫혀 있다. */
    public static final String TAG_ARCHIVE_OUTCOME = "outcome";

    public static final String ARCHIVE_INSERTED = "inserted";
    public static final String ARCHIVE_DUPLICATE = "duplicate";

    /** 계약 위반의 종류. 값은 {@code unknown_enum · unsupported_schema · other} 셋으로 닫혀 있다. */
    public static final String VIOLATION_UNKNOWN_ENUM = "unknown_enum";
    public static final String VIOLATION_UNSUPPORTED_SCHEMA = "unsupported_schema";
    public static final String VIOLATION_OTHER = "other";

    /** 수집 경로 라벨. 값은 {@link #PATH_CONSISTENCY} · {@link #PATH_STOCK} 둘로 고정이다. */
    public static final String TAG_COLLECT_PATH = "path";

    public static final String PATH_CONSISTENCY = "consistency";
    public static final String PATH_STOCK = "stock";

    private DomainMeterNames() {
    }

    /**
     * gap 종류를 {@link #TAG_GAP_TYPE} 라벨 값으로 바꾼다.
     *
     * <p>enum 이름을 그대로 쓰지 않는 이유 — 라벨 값은 PromQL 질의와 패널에 박히는 공개 계약이라
     * enum 상수명 리팩터링이 대시보드를 조용히 끊으면 안 된다.
     *
     * @param gapType 라벨로 바꿀 gap 종류
     * @return 소문자 라벨 값
     */
    public static String gapTagValue(ConsistencyGapType gapType) {
        return switch (gapType) {
            case ACTIVE_DB_GAP -> "active_db";
            case LUA_GAP -> "lua";
            case PERSIST_GAP -> "persist";
            case DB_COUNTER_GAP -> "db_counter";
        };
    }
}
