// 밀린 회차 전이 수를 DB 에서 되읽어 게이지로 냅니다.
package com.kafkick.batch.config;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.port.CouponRoundTransitionRepository;
import com.kafkick.core.coupon.port.CouponRoundTransitionRepository.PendingCounts;
import com.kafkick.core.support.TimeProvider;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * <b>{@code CouponRoundScheduler} 는 배치 메타를 안 남긴다.</b> Spring Batch 잡이 아니라
 * {@code @Scheduled} 이므로 <i>"마지막으로 성공한 실행"</i> 이 어디에도 없고,
 * {@code cy_batch_last_success_seconds} 축을 쓸 수 없다.
 *
 * <p>그래서 <b>실행 여부가 아니라 결과를 본다</b> — <i>"열려야 하는데 안 열린 회차가 몇인가"</i>.
 * 정상 상태에서 0 이고, 스케줄러가 죽거나 전이가 계속 실패하면 1분마다 올라간다.
 * CY-392 가 {@code BatchJobNotRunning}(증분 창의 델타가 0인지 보는 방식)을 없앤 이유가
 * 이것이다 — <b>"안 돌았다" 는 정상과 고장을 구분하지 못한다.</b>
 *
 * <p><b>이 게이지는 스케줄러가 꺼져 있어도 나간다.</b> 이 빈에는
 * {@code @ConditionalOnProperty} 를 안 걸었다 — {@code batch.scheduling.enabled=false} 로
 * 띄운 배치에서도 <i>"회차가 안 열리고 있다"</i> 는 사실은 참이고, 그것을 숨기면 끈 것을
 * 아무도 알림에 말해 주지 않는다. 형제 되읽기 둘이 같은 이유로 같은 모양이다.
 * 부하 테스트는 컨테이너를 아예 안 띄우므로 게이지가 사라지고, {@code absent()} 알림이
 * {@code up{job="cy-batch"}} 로 그 구간을 걸러 낸다.
 *
 * <p><b>실패는 {@code NaN} 으로 낸다.</b> 0 으로 내면 <i>"밀린 것이 없다"</i> 가 되어 감시가
 * 조용히 꺼진다 — 모르는 것과 없는 것은 다르다. {@code NaN} 은 시리즈로 존재하므로
 * {@code absent()} 로는 못 잡고, 알림이 {@code x != x} 로 본다.
 */
@Component
public class CouponRoundPendingRefresher {

    private static final long MIN_REFRESH_MILLIS = 10_000;

    /**
     * <b>형제 둘과 같은 120초다.</b> 한때 300초를 두고 그 근거를 <i>"5분을 넘으면 for 5m 창에
     * 샘플이 한 번도 안 들어와 타이머가 못 찬다"</i> 로 적었는데 <b>그건 틀렸다</b> —
     * Prometheus 는 {@code scrape_interval: 15s} 로 값을 <b>당겨 가고</b>, 게이지 람다는 캐시된
     * 스냅샷을 읽어 낸다. 되읽기 주기가 5분이어도 시리즈에는 15초마다 샘플이 들어온다.
     *
     * <p>실제 대가는 <b>값의 나이</b>다. 조건이 생긴 직후 되읽기가 막 지나갔으면 최대 이 주기만큼
     * 탐지가 늦고, 사람이 고친 뒤에도 이 주기만큼 {@code critical} 이 서 있다(이쪽이 진짜 오탐이다).
     * 그리고 {@code CouponRoundMetricsStale} 이 10분 창에서 시도 횟수를 세므로, 300초면
     * 시도가 두 번뿐이라 임계 3 을 <b>구조적으로 못 채운다.</b>
     */
    private static final long MAX_REFRESH_MILLIS = 120_000;

    private static final double UNKNOWN = Double.NaN;

    private static final Logger log = LoggerFactory.getLogger(CouponRoundPendingRefresher.class);

    private final CouponRoundTransitionRepository rounds;
    private final TimeProvider timeProvider;
    private final Counter refreshFailures;

    /** 스냅샷 하나를 통째로 갈아 끼운다 — 근거는 {@link Snapshot} 에 있다. */
    private final AtomicReference<Snapshot> latest = new AtomicReference<>();

    /**
     * <b>첫 실패만 스택을 남긴다.</b> 커넥션 풀이 마르면 60초마다 같은 스택이 쌓여 다른 로그를
     * 밀어내는데, 이 티켓이 만든 runbook 다섯(NotOpening · NotClosing · SelectFailing · MetricsStale · MetricsUnknown)이 사람을 그 로그로 보낸다. 반대로 <b>첫 발생은
     * 곧 원인 조사 시점</b>이라 타입 없이는 못 읽는 실패가 있다({@code getMessage()} 가
     * {@code null} 인 계열). 형제 둘이 같은 모양이다.
     */
    private final AtomicLong failureStreak = new AtomicLong();

    /**
     * 스케줄러 스레드에서 도는 조회라 끊을 수단이 없으면 커넥션 풀이 마른 날 다음 주기도
     * 안 돈다. 읽기 전용으로 열어 데드라인을 심는다. 형제 되읽기 둘과 같은 모양이다.
     */
    private final TransactionTemplate readPending;

    /**
     * <b>넷을 한 홀더에 담는다.</b> 따로 두면 되읽기가 중간에 실패한 주기에 일부만 갱신돼
     * <b>서로 다른 시각의 값</b>이 나란히 나가고, 합이 안 맞는 조합이 관제에 뜬다.
     */
    private record Snapshot(int pendingOpen, int pendingClose, int missedWindow,
            int blockedByMissingStock) {
    }

    public CouponRoundPendingRefresher(CouponRoundTransitionRepository rounds, TimeProvider timeProvider,
            MeterRegistry registry, PlatformTransactionManager transactionManager,
            @Value("${batch.metrics.coupon-round-refresh-ms:60000}") long refreshMillis,
            @Value("${batch.metrics.coupon-round-timeout-ms:5000}") long timeoutMillis,
            @Value("${batch.scheduling.enabled:false}") String schedulingEnabledRaw) {
        if (refreshMillis < MIN_REFRESH_MILLIS || refreshMillis > MAX_REFRESH_MILLIS) {
            throw new IllegalArgumentException(
                    "batch.metrics.coupon-round-refresh-ms 는 " + MIN_REFRESH_MILLIS + "~"
                            + MAX_REFRESH_MILLIS + " 이어야 합니다. 늘리면 **값의 나이**가 "
                            + "그만큼 늘어 탐지가 늦고 해소 뒤에도 알림이 서 있으며, "
                            + "CouponRoundMetricsStale 이 10분 창에서 시도 횟수를 세므로 "
                            + "임계를 구조적으로 못 채웁니다. 줄이면 1분 크론보다 자주 "
                            + "coupons 를 칩니다. 받은 값=" + refreshMillis);
        }
        // 스프링 트랜잭션 타임아웃이 초 단위라 999 이하는 0 으로 잘리는데, 0 은 "무제한" 이
        // 아니라 데드라인이 이미 지났음이다. 형제와 같은 가드다.
        if (timeoutMillis < 1_000 || timeoutMillis % 1_000 != 0) {
            throw new IllegalArgumentException(
                    "batch.metrics.coupon-round-timeout-ms 는 1000 이상이면서 1000 의 "
                            + "배수여야 합니다. 받은 값=" + timeoutMillis);
        }
        this.rounds = rounds;
        this.timeProvider = timeProvider;
        this.refreshFailures = Counter.builder("cy_coupon_round_refresh_failures_total")
                .description("회차 전이 대기 지표 되읽기가 실패한 횟수")
                .register(registry);
        this.readPending = new TransactionTemplate(transactionManager);
        this.readPending.setReadOnly(true);
        // 집계가 한 문장이라 RC 여도 스냅샷이 하나다. RR 이면 60초마다 스냅샷을 새로 잡느라
        // 언두를 더 오래 붙든다 — 형제 되읽기가 같은 판단을 했다.
        this.readPending.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.readPending.setTimeout(Math.toIntExact(timeoutMillis / 1_000));

        // **끈 것을 알림에 말해 준다.** 이 빈은 batch.scheduling.enabled 와 무관하게 뜨는데,
        // 끈 채로 배치를 띄우면 회차가 안 열리는 것이 **사실**이라 대기 게이지가 올라간다.
        // 그것을 critical 로 내보내면 결정론 증명 구간(그 플래그를 끄는 것이 정상 절차다)
        // 내내 알림이 하나 켜져 있고, 곧 무시되기 시작한다 — 이 저장소가 오염 스키마 기동에
        // 대해 이미 같은 판단을 했다(ExpirePendingRefresher 의 cleanSchema 갈래).
        // 그래서 상태를 지표로 내고 알림이 그것으로 갈래를 뺀다.
        //
        // **부팅 시점 값 하나로 충분하다.** 이 프로퍼티를 소비하는 것은 세 스케줄러의
        // @ConditionalOnProperty 뿐이라 **빈의 존재가 부팅에 고정된다** — 런타임에 값을
        // 바꿔도 스케줄러가 생기거나 사라지지 않는다. 게다가 actuator 노출이
        // health,metrics,prometheus 화이트리스트라 /actuator/env 도 refresh 도 안 열린다.
        //
        // ⚠️ **@ConditionalOnProperty 와 같은 술어로 판정한다.** @Value 의 boolean 변환은
        //    "1"·"yes"·"on" 도 참으로 읽는데, OnPropertyCondition 은
        //    havingValue.equalsIgnoreCase(value) 하나뿐이라 그 셋을 거짓으로 본다 —
        //    그대로 두면 BATCH_SCHEDULING_ENABLED=1 인 배포에서 **게이지는 켜짐인데
        //    스케줄러 빈은 없는** 상태가 되고, 알림이 그 갈래를 빼 버려 조용해진다.
        //    batch.yml 이 그 값을 환경변수로 그대로 받으므로 실제 경로다.
        boolean schedulingEnabled = "true".equalsIgnoreCase(schedulingEnabledRaw);
        Gauge.builder("cy_coupon_round_scheduling_enabled", () -> schedulingEnabled ? 1 : 0)
                .description("회차 상태 전이 스케줄러가 켜져 있는가 — 1 켜짐 · 0 꺼짐")
                .register(registry);

        gauge(registry, "cy_coupon_round_pending_open", Snapshot::pendingOpen,
                "지금 열려 있어야 하는데 아직 SCHEDULED 인 회차 수 — 재고 없는 회차와 창이 "
                        + "지난 회차는 빼고 센다");
        gauge(registry, "cy_coupon_round_pending_close", Snapshot::pendingClose,
                "close_at 이 지났는데 아직 OPEN 인 회차 수");
        gauge(registry, "cy_coupon_round_missed_window", Snapshot::missedWindow,
                "SCHEDULED 인데 close_at 도 이미 지난 회차 수 — 창을 통째로 놓쳤다");
        gauge(registry, "cy_coupon_round_blocked_no_stock", Snapshot::blockedByMissingStock,
                "열려야 하지만 coupon_stocks 에 행이 없어 못 여는 회차 수 — 데이터 축이다");
    }

    /**
     * <b>루프 전체를 감싼다.</b> 조회가 끊기지 않고 블록되면 {@code markFailed} 에 도달하지
     * 못해 게이지가 {@code NaN} 이 아니라 <b>낡은 값으로 얼어붙는다</b> — 그러면 {@code NaN} 을
     * 보는 알림도 실패 카운터를 보는 알림도 그것을 못 본다. 형제 둘이 같은 이유로 같은 모양이다.
     */
    @Scheduled(fixedDelayString = "${batch.metrics.coupon-round-refresh-ms:60000}",
            initialDelayString = "${batch.metrics.coupon-round-initial-delay-ms:0}")
    public void refresh() {
        try {
            LocalDateTime now = timeProvider.now();
            // **한 문장으로 센다.** 넷을 나눠 세면 READ COMMITTED 에서 문장마다 read view 가
            // 새로 잡혀 서로 다른 시점의 값이 나란히 나간다 — 홀더를 하나로 묶는 것은
            // 발행의 원자성이지 읽기의 일관성이 아니다. 근거는 포트 javadoc 에 적었다.
            PendingCounts counts = readPending.execute(ignored -> rounds.countPending(now));
            Snapshot fresh = new Snapshot(counts.pendingOpen(), counts.pendingClose(),
                    counts.missedWindow(), counts.blockedByMissingStock());
            latest.set(fresh);
            failureStreak.set(0);
        } catch (RuntimeException e) {
            markFailed(e);
        } catch (Error e) {
            // **게이지는 떨어뜨리되 삼키지 않는다.** 여기서 안 잡으면 latest 가 낡은 값으로
            // 얼어붙고, NaN 을 보는 알림도 카운터를 보는 알림도 그것을 못 본다 —
            // 이 클래스가 막겠다고 적은 바로 그 상태다. 형제 둘이 같은 모양이다.
            markFailed(e);
            throw e;
        }
    }

    private void markFailed(Throwable cause) {
        refreshFailures.increment();
        latest.set(null);
        if (failureStreak.getAndIncrement() == 0) {
            log.warn("회차 전이 대기 지표를 되읽지 못했습니다. 게이지를 NaN 으로 둡니다.", cause);
        } else {
            log.warn("회차 전이 대기 지표를 되읽지 못했습니다. 게이지를 NaN 으로 둡니다. 원인={}",
                    cause.toString());
        }
    }

    private void gauge(MeterRegistry registry, String name,
            java.util.function.ToIntFunction<Snapshot> field, String description) {
        Gauge.builder(name, latest, holder -> {
                    Snapshot snapshot = holder.get();
                    return snapshot == null ? UNKNOWN : field.applyAsInt(snapshot);
                })
                .description(description)
                .register(registry);
    }
}
