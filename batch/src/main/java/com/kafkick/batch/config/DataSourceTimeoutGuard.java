// JDBC 소켓 타임아웃이 가장 긴 Step 데드라인을 덮는지 기동 때 확인합니다.
package com.kafkick.batch.config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * <b>{@code socketTimeout} 이 짧으면 정상 배치가 죽는다.</b> 그 값은 <b>소켓 읽기</b> 상한이라
 * 오래 도는 질의를 구분하지 않는다 — {@code docs/12} 의 실측으로 {@code replayStep} 이
 * <b>312초</b>이고 {@code batch.verify.step-timeout-ms} 는 <b>600초</b>다. 그보다 작게 잡으면
 * 검증이 매번 같은 자리에서 끊기고, 원인은 배치가 아니라 URL 한 글자에 있다.
 *
 * <p><b>반대로 없으면 무기한 멈춘다.</b> Hikari 의 {@code connection-timeout}(3초)은
 * <b>풀에서 빌리는</b> 시간이지 소켓 시간이 아니다. TCP 는 살아 있는데 응답이 안 오는 상태
 * (방화벽 drop · 서버 멎음)에서 빌려주기는 이미 끝났으므로 그 가드가 안 걸린다.
 * {@link SchemaPresenceGuard} 는 {@code @Order(HIGHEST_PRECEDENCE)} 러너라 거기서 멈추면
 * <b>컨테이너는 "떠 있음" 인데 {@code @Scheduled} 여덟이 하나도 안 돈다</b> —
 * {@code CouponRoundScheduler} 까지 서서 발급 문이 안 열린다.
 *
 * <p>그래서 <b>둘 다 있어야 하고, 하나는 충분히 커야 한다.</b> 그 관계를 기동 때 못 박는다 —
 * {@link RunningJobProbe} 가 {@code stuck-job-after-ms} 에 거는 가드와 같은 모양이다.
 *
 * <p><b>왜 생성자가 아니라 {@code @Component} 인가.</b> 이 검사는 빈 하나의 불변식이 아니라
 * <b>설정 둘 사이의 관계</b>다. 어느 한쪽 빈에 얹으면 그 빈이 안 만들어지는 프로파일에서
 * 조용히 사라진다.
 */
@Component
public class DataSourceTimeoutGuard {

    private static final Logger log = LoggerFactory.getLogger(DataSourceTimeoutGuard.class);

    private static final Pattern SOCKET_TIMEOUT = Pattern.compile("[?&]socketTimeout=(\\d+)");

    /** 게이지가 읽는 값. 러너가 한 번만 도므로 필드에 남긴다. */
    private volatile boolean verified;

    private static final Pattern CONNECT_TIMEOUT = Pattern.compile("[?&]connectTimeout=(\\d+)");

    /** 거절을 끄는 손잡이. 기본은 켬 — 끄는 법은 거절 메시지가 직접 말한다. */
    static final String REQUIRED = "batch.datasource-timeout-guard.required";

    public DataSourceTimeoutGuard(
            DataSource dataSource,
            @Value("${" + REQUIRED + ":true}") boolean required,
            @Value("${batch.verify.step-timeout-ms:600000}") long verifyStepTimeoutMs,
            @Value("${batch.expire.step-timeout-ms:120000}") long expireStepTimeoutMs,
            @Value("${batch.cleanup.step-timeout-ms:120000}") long cleanupStepTimeoutMs,
            MeterRegistry registry) {

        // **프로퍼티가 아니라 실물에서 읽는다.** 테스트는 @ServiceConnection 이 DataSource 를
        // 프로그램으로 만들어 spring.datasource.url 이 아예 없다 — 프로퍼티로 읽으면 그 자리에서
        // PlaceholderResolutionException 이 나고, 그러면 이 가드가 **테스트에서만** 무너진다.
        // 실물에서 읽으면 운영·테스트가 같은 문자열을 본다.
        String url = jdbcUrlOf(dataSource);
        if (url == null) {
            publish(registry, false);
            log.warn("DataSource 에서 JDBC URL 을 못 읽어 타임아웃 검사를 건너뜁니다. "
                    + "getJdbcUrl() 이 없는 구현입니다. type={}",
                    dataSource.getClass().getName());
            return;
        }

        Matcher connect = CONNECT_TIMEOUT.matcher(url);
        if (connect.find() && Long.parseLong(connect.group(1)) == 0) {
            // socketTimeout 과 같다 — Connector/J 에서 0 은 "없음" 이 아니라 **무제한**이다.
            // 존재만 보면 이 값이 통과해 가드의 핵심 보장이 그대로 깨진다.
            reject(registry, required,
                    "connectTimeout=0 은 무제한입니다 — 연결 수립이 무기한 걸릴 수 있고, "
                            + "그때 SchemaPresenceGuard(기동 러너)에서 멈춰 컨테이너는 떠 "
                            + "있는데 @Scheduled 가 하나도 안 돕니다. 유한한 값을 환경변수 "
                            + "DB_CONNECT_TIMEOUT_MS 로 주십시오.");
            return;
        }
        if (!connect.reset().find()) {
            reject(registry, required,
                    "spring.datasource.url 에 connectTimeout 이 없습니다. 없으면 연결 수립이 "
                            + "무기한 걸릴 수 있고, 그때 SchemaPresenceGuard(기동 러너)에서 "
                            + "멈춰 컨테이너는 떠 있는데 @Scheduled 가 하나도 안 돕니다. "
                            + "환경변수 DB_CONNECT_TIMEOUT_MS 로 줍니다.");
            return;
        }

        long longest = verifyStepTimeoutMs;
        String longestKey = "batch.verify.step-timeout-ms";
        if (expireStepTimeoutMs > longest) {
            longest = expireStepTimeoutMs;
            longestKey = "batch.expire.step-timeout-ms";
        }
        if (cleanupStepTimeoutMs > longest) {
            longest = cleanupStepTimeoutMs;
            longestKey = "batch.cleanup.step-timeout-ms";
        }
        Matcher socket = SOCKET_TIMEOUT.matcher(url);
        if (!socket.find()) {
            reject(registry, required,
                    "spring.datasource.url 에 socketTimeout 이 없습니다. Hikari 의 "
                            + "connection-timeout 은 풀 대여 시간이라 응답 없는 소켓을 "
                            + "못 끊습니다 — " + longestKey + "(" + longest + "ms)보다 큰 값을 "
                            + "환경변수 DB_SOCKET_TIMEOUT_MS 로 주십시오.");
            return;
        }

        long socketTimeoutMs = Long.parseLong(socket.group(1));
        // **0 은 "작다" 가 아니라 "무제한" 이다**(Connector/J 규약). 이유가 정반대이므로
        // 문구를 갈라야 한다 — 같은 메시지를 내면 운영자가 값을 더 키우려 든다.
        if (socketTimeoutMs == 0) {
            reject(registry, required,
                    "socketTimeout=0 은 무제한입니다 — 이 가드가 막으려는 상태 그 자체입니다. "
                            + longestKey + "(" + longest + "ms)보다 큰 유한한 값을 "
                            + "환경변수 DB_SOCKET_TIMEOUT_MS 로 주십시오.");
            return;
        }
        if (socketTimeoutMs <= longest) {
            reject(registry, required,
                    "socketTimeout 은 가장 긴 Step 데드라인보다 커야 합니다. 작으면 "
                            + "정상 배치가 매번 같은 자리에서 끊기고, 원인이 배치가 아니라 "
                            + "URL 에 있어 찾기 어렵습니다(실측: replayStep 312초, docs/12). "
                            + "socketTimeout=" + socketTimeoutMs + " " + longestKey
                            + "=" + longest + "ms — 그 값을 올렸다면 "
                            + "DB_SOCKET_TIMEOUT_MS 도 함께 올리십시오.");
            return;
        }

        publish(registry, true);
        log.info("JDBC 타임아웃 확인 완료 — socketTimeout={}ms 가 가장 긴 Step 데드라인({}ms)을 "
                + "덮습니다.", socketTimeoutMs, longest);
    }

    /**
     * <b>끈 상태를 지표로 낸다.</b> 이 스택에는 Loki·promtail 이 없어 <b>로그가 감시 수단이
     * 아니다</b> — {@code batch-alerts.yml} 이 인덱스 축에 대해 같은 문장을 이미 적어 뒀다.
     * ERROR 한 줄로만 남기면 며칠 뒤 그 줄에 닿는 사람이 없다.
     * {@code cy_batch_schema_index_enforcement} 와 같은 모양이다.
     */
    private void publish(MeterRegistry registry, boolean verified) {
        this.verified = verified;
        if (registry == null) {
            return;
        }
        Gauge.builder("cy_batch_jdbc_timeout_verified", this, self -> self.verified ? 1 : 0)
                .description("JDBC 타임아웃 검사를 통과했는가 — 1 통과 · 0 못 했거나 껐다")
                .register(registry);
    }

    /**
     * <b>기본은 거절, 끄면 ERROR.</b> {@link SchemaPresenceGuard} 의 인덱스 축과 같은 모양이다 —
     * 이 가드가 막으려는 사고(기동 러너에서 멈춰 {@code @Scheduled} 가 다 서는 것)를
     * <b>가드 자신이 일으킬 수 있는 자리</b>라, 끄는 손잡이가 없으면 되돌릴 방법이 없다.
     * 끈 상태는 조용하지 않게 ERROR 로 남긴다.
     */
    private void reject(MeterRegistry registry, boolean required, String message) {
        if (required) {
            throw new IllegalStateException(message
                    + " 지금 당장 띄워야 하면 환경변수 "
                    + "DATASOURCE_TIMEOUT_GUARD_REQUIRED=false (또는 실행 인자 --"
                    + REQUIRED + "=false) 로 거절을 끌 수 있습니다.");
        }
        publish(registry, false);
        log.error("JDBC 타임아웃 검사에 걸렸습니다 — 거절은 꺼져 있습니다({}=false). {}",
                REQUIRED, message);
    }

    /**
     * <b>커넥션을 열지 않는다.</b> {@code getMetaData().getURL()} 은 실제 접속을 하나 여는데,
     * 이 가드가 잡으려는 상황이 <b>바로 그 접속이 안 돌아오는 것</b>이다 — 거기서 읽으면
     * 검사기가 검사 대상과 같은 자리에서 멈춘다. 풀은 설정값을 그대로 들고 있으므로
     * 접속 없이 읽는다.
     *
     * <p><b>리플렉션인 이유.</b> {@code batch} 의 컴파일 클래스패스에 Hikari 가 없다
     * (JDBC 는 {@code storage} 를 통해 런타임에만 온다). 가드 하나 때문에 의존을 늘리는 대신
     * 이름으로 찾는다 — 못 찾으면 던지지 않고 <b>WARN 으로 남기고 통과</b>시킨다.
     * 여기서 기동을 막으면 풀 구현을 바꾸는 날 배치가 통째로 안 뜬다.
     */
    private static String jdbcUrlOf(DataSource dataSource) {
        try {
            Object url = dataSource.getClass().getMethod("getJdbcUrl").invoke(dataSource);
            return url instanceof String text ? text : null;
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }
}
