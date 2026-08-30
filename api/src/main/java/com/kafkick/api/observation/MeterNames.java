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

    // ── OBS-25 가 직접 만드는 캠페인 발급 미터 ─────────────────────────────
    public static final String ISSUANCE_FLOW = "app.issuance.flow";
    public static final String QUEUE_ADMITTED = "app.queue.admitted";
    public static final String ISSUANCE_OUTCOME = "app.issuance.outcome";
    public static final String ISSUANCE_EVENT_LAST_SUCCESS_EPOCH =
            "app.issuance.event.last.success.epoch";
    public static final String QUEUE_EVENT_LAST_ADMITTED_EPOCH =
            "app.queue.event.last.admitted.epoch";
    public static final String CAMPAIGN_LIMIT_EXCEEDED =
            "app.observation.campaign.limit.exceeded";

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
