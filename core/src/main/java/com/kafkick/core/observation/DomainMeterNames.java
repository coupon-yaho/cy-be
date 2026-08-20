package com.kafkick.core.observation;

import com.kafkick.core.consistency.ConsistencyGapType;

/**
 * batch 가 등록하고 조회 API(OBS-6)가 읽는 도메인 Gauge 이름. 양쪽이 문자열을 각자 옮겨 적으면
 * 한쪽만 바뀌어도 예외 없이 "값 없음" 만 오고 앱은 정상 기동한다 — 로그도 없이 화면만 빈다.
 *
 * <p>등록하는 쪽은 batch 이고 읽는 쪽은 api 라 두 모듈이 함께 보는 core 에 둔다.
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

    // TODO(OBS-17 후속 티켓): Kafka persist lag 미터가 여기 하나 더 붙는다. 관측-Batch 포트 계약은
    //   batch 산출물을 4종(정합성 gap · 대기열 · 재고 · Kafka persist lag)으로 적는데, 이 티켓은
    //   앞의 3종만 낸다 — lag 은 AdminClient 배선이 필요하고 그건 OBS-17 소유다. 그 통로가 열리는
    //   시점은 BatchKafkaLagAssumptionTest 가 알려 준다.

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
