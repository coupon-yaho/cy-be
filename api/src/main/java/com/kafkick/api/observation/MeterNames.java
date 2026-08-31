package com.kafkick.api.observation;

/**
 * 미터 이름 상수. 지표를 읽는 쪽(OBS-4 · OBS-8 · OBS-10 · OBS-11)은 문자열을 직접 쓰지 말고
 * 여기를 참조한다. 이름이 어긋나면 예외 없이 "값 없음"만 오고 앱은 정상 기동한다 — 로그도
 * 없이 화면만 빈다.
 *
 * <p><b>컴파일러가 잡아 주는 범위는 참조가 생긴 뒤부터다.</b> 아직 등록 코드가 없는 이름은
 * 등록하는 쪽과 읽는 쪽이 같은 문자열을 쓰도록 미리 합의해 두는 역할만 한다.
 *
 * <p>{@link #HTTP_SERVER_REQUESTS} 는 observation.yml 의 백분위 · expiry 키와 같은 문자열이어야
 * 한다. 그 계약은 {@code ObservationMetricsContractTest} 가 두 파일을 함께 열어 검증한다.
 */
public final class MeterNames {

    // ── OBS-4 가 직접 만드는 미터 ───────────────────────────────────────
    public static final String HTTP_LATENCY = "app.http.latency";
    public static final String HTTP_RESULT = "app.http.result";
    public static final String IN_FLIGHT = "app.http.inflight";

    // ── 락 경합 재시도. 발급·사용·사용취소·발급취소가 operation 태그로 갈린다 ──────
    public static final String COUPON_LOCK_RETRY = "coupon.lock.retry";

    // ── OBS-25 가 직접 만드는 쿠폰 회차 발급 미터 ─────────────────────────────
    public static final String ISSUANCE_FLOW = "app.issuance.flow";
    public static final String QUEUE_ADMITTED = "app.queue.admitted";
    public static final String ISSUANCE_OUTCOME = "app.issuance.outcome";
    public static final String ISSUANCE_EVENT_LAST_SUCCESS_EPOCH =
            "app.issuance.event.last.success.epoch";
    public static final String QUEUE_EVENT_LAST_ADMITTED_EPOCH =
            "app.queue.event.last.admitted.epoch";
    // ── v2 발급의 중복·재시도 카운터. 셋을 합치지 않는다(문서 04) ──────────────
    public static final String ISSUANCE_V2_DUP_PER_MEMBER = "app.issuance.v2.dup.per.member";
    public static final String ISSUANCE_V2_REPLAY_DONE = "app.issuance.v2.replay.done";
    public static final String ISSUANCE_V2_REPLAY_PENDING = "app.issuance.v2.replay.pending";
    /** Redis 선점은 성공했지만 DB 최종 재고가 매진으로 막은 요청. 복제 유실의 직접 신호다. */
    public static final String ISSUANCE_V2_DATABASE_STOCK_DIVERGENCE =
            "app.issuance.v2.database.stock.divergence";
    /** Redis 게이트가 통과시킨 회원을 DB uk_coupon_member 가 막은 요청. 복제 유실의 직접 신호다. */
    public static final String ISSUANCE_V2_DATABASE_MEMBER_DIVERGENCE =
            "app.issuance.v2.database.member.divergence";
    /**
     * DB 가 막아 거절했는데 Redis 선점이 되돌아오지 않은 요청. 그 회차의 Redis 재고가
     * 그만큼 영구히 낮아진다 — 과소 발급 방향이라 응답에는 드러나지 않는다.
     */
    public static final String ISSUANCE_V2_CLAIM_LEAKED = "app.issuance.v2.claim.leaked";
    /**
     * 보상이 되돌릴 선점을 <b>찾지 못한</b> 요청(보상 Lua {@code 2}). 다른 절차가 이미
     * 정리한 것이라 이 요청이 게이트에 남긴 선점은 없다 — 누수가 아니다. 남의 토큰이 덮은
     * 경우({@code 0})는 갈려 나가 {@link #ISSUANCE_V2_CLAIM_LEAKED} 로 간다(CY-781).
     */
    public static final String ISSUANCE_V2_COMPENSATION_NO_CLAIM =
            "app.issuance.v2.compensation.no.claim";
    /**
     * 이미 완료 승격된 선점에 보상이 도달한 요청. <b>이 요청이 되돌릴 것은 없으나</b>, DB
     * 트랜잭션이 롤백된 경로였다면 그 회차 Redis 재고가 한 장 낮아진 채 남는다 — 경보 대상이다.
     */
    public static final String ISSUANCE_V2_COMPENSATION_ALREADY_DONE =
            "app.issuance.v2.compensation.already.done";
    /** Redis failover·차단기·보상 불일치로 COUPON-325를 반환한 요청. */
    public static final String ISSUANCE_V2_REDIS_UNAVAILABLE = "app.issuance.v2.redis.unavailable";

    public static final String COUPON_ROUND_LIMIT_EXCEEDED =
            "app.observation.coupon.round.limit.exceeded";

    // ── 자동 계측. 아래 이름은 우리가 정한 게 아니라 Micrometer 바인더가 정한 것이다 ──
    /** Spring Boot 자동 계측 Timer. observation.yml 이 이 이름으로 백분위 · expiry 를 건다. */
    public static final String HTTP_SERVER_REQUESTS = "http.server.requests";

    public static final String HIKARI_ACTIVE = "hikaricp.connections.active";
    public static final String HIKARI_PENDING = "hikaricp.connections.pending";
    /** 풀 정원. 사용률의 분모라 값 미터만으로는 포화를 판정할 수 없다. */
    public static final String HIKARI_MAX = "hikaricp.connections.max";
    public static final String JVM_MEMORY_USED = "jvm.memory.used";
    /**
     * 영역별 상한. G1 에서는 Eden · Survivor 가 {@code -1} 을 내고 Old Gen 만 힙 전체 상한을
     * 낸다(실측) — 영역 값을 더하면 안 된다.
     */
    public static final String JVM_MEMORY_MAX = "jvm.memory.max";
    public static final String CPU_USAGE = "process.cpu.usage";
    /** {@code server.tomcat.mbeanregistry.enabled=true} 일 때만 등록된다. */
    public static final String TOMCAT_BUSY = "tomcat.threads.busy";
    /** {@code server.tomcat.mbeanregistry.enabled=true} 일 때만 등록된다. */
    public static final String TOMCAT_MAX = "tomcat.threads.config.max";

    // ── 상태 전이·알림 카운터. 등록은 A 가 하고 Prometheus 가 함께 긁는다 ──────────
    // public static final String ISSUANCE_EXPIRED = "app.issuance.expired";
    public static final String NOTIFY_SENT =
            com.kafkick.core.observation.DomainMeterNames.NOTIFY_SENT;

    private MeterNames() {
    }
}
