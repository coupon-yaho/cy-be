// 만료 실행의 결과를 관제가 읽을 수 있게 내보냅니다.
package com.kafkick.batch.config;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToDoubleFunction;

import org.springframework.stereotype.Component;

import com.kafkick.core.expiration.PendingExpiration;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * <b>잡의 생사만으로는 "성공했는데 아무것도 안 했다" 를 못 잡는다.</b>
 *
 * <p>기존 알림 셋({@code BatchJobFailed}·{@code BatchJobNotRunning}·
 * {@code BatchJobRunningTooLong})은 전부 그 축이다. 셋 다 통과하면서 기한 지난 발급건이
 * 계속 쌓이는 상태가 있고, 그것이 이 잡의 가장 나쁜 실패 모드다 — 재고를 되돌리는 유일한
 * 배치가 조용히 일을 안 한다.
 *
 * <p><b>게이지를 셋으로 가르는 것이 핵심이다.</b> 막힌 회차의 몫은 설계상 계속 남는다.
 * 합쳐서 알림을 걸면 사람이 재고를 고칠 때까지 24시간 울리고, 그 알림은 곧 무시된다.
 *
 * <pre>
 *   cy_expire_pending             기한 지났는데 ISSUED 인 전체
 *   cy_expire_blocked_pending     그중 이번 실행이 건너뛴 회차의 몫
 *   cy_expire_unexplained_pending 그 둘의 차. <b>알림이 보는 것은 이것 하나다</b>
 *   cy_expire_blocked_coupons     건너뛴 회차 수
 * </pre>
 *
 * <p><b>차를 관제가 아니라 여기서 뺀다.</b> 게이지 둘을 내보내고 알림 식이
 * {@code pending - blocked_pending} 을 하면, 두 값을 따로 {@code set} 하는 사이에
 * 스크레이프가 끼어 <b>한쪽만 새 값인 샘플</b>이 나온다. 값 자체는 한 문장에서 함께
 * 세어 왔는데(그러려고 {@code COUNT_PENDING} 을 한 문장으로 만들었다) 마지막 한 걸음에서
 * 다시 갈리는 것이다. 뺄셈을 여기서 해 <b>한 시계열</b>로 내보내면 그 틈이 없다.
 * 나머지 셋은 진단용으로 남긴다.
 *
 * <p><b>이름에 {@code _total} 을 붙이지 않는다.</b> 그것은 카운터 규약이라 Micrometer 의
 * Prometheus 렌더러가 게이지에서는 <b>떼어 낸다</b> — 붙여 두면 코드가 부르는 이름과 관제가
 * 보는 이름이 갈리고, 알림 규칙이 영원히 안 뜬다. 실제로 그렇게 만들었다가 노출 테스트가 잡았다.
 *
 * <p>그래서 알림이 둘로 갈린다 — {@code unexplained_pending > 0} 은
 * <b>배치가 일을 안 한다</b>(서버를 본다), {@code blocked_coupons > 0} 은
 * <b>데이터가 어긋나 있다</b>(데이터를 본다). 설계가 정한
 * <i>"서버를 고칠 상황과 데이터를 볼 상황을 같은 알람으로 묶지 않는다"</i> 가 이것이다.
 *
 * <p><b>스크레이프 때 세지 않는다.</b> 300만 행에 {@code COUNT(*)} 를 15초마다 때리는 꼴이다.
 * 값은 잡이 끝날 때 한 번 밀어 넣고, 게이지는 그 값을 들고만 있는다.
 *
 * <p><b>실행 전에는 값이 없다.</b> 기동 직후 스크레이프가 0 을 읽으면 <i>"밀린 것이 없다"</i>
 * 로 오해된다 — 아직 모르는 것과 없는 것은 다르다. 그래서 첫 실행 전까지 {@code NaN} 을 내고,
 * <b>판정할 재료가 없었던 실행</b>도 {@link #markUnknown()} 으로 여기로 되돌린다 —
 * 오염 스키마를 보고 있었거나, Step 을 시작도 못 했거나, 세다가 죽은 경우다.
 *
 * <p><b>{@code NaN} 이 조용한 이유를 정확히 적어 둔다 — 시리즈가 없어서가 아니다.</b>
 * Prometheus 는 {@code NaN} 을 <b>값으로 저장한다.</b> 시리즈는 존재하고
 * {@code absent()} 는 발화하지 않는다. 알림이 조용한 것은 {@code NaN > 0} 이 거짓이기
 * 때문이다. 이 차이가 중요한 것은 <b>{@code NaN} 이 집계에 전염되기</b> 때문이다 —
 * 인스턴스를 여럿으로 늘려 {@code sum()}·{@code avg_over_time()} 을 걸면 아직 한 번도
 * 안 돈 인스턴스 하나가 <b>집계 전체를 침묵시킨다.</b> 그때는
 * {@code cy_expire_unexplained_pending != NaN} 로 먼저 걸러야 한다.
 */
@Component
public class ExpireMetrics {

    private static final double UNKNOWN = Double.NaN;

    /**
     * 한 실행이 낸 넷. <b>넷을 한 덩어리로 바꾸는 것이 핵심이다.</b>
     *
     * <p>{@code AtomicLong} 넷을 따로 {@code set} 하면 그 사이에 스크레이프가 끼어
     * <b>서로 다른 실행의 값이 섞인 샘플</b>이 나온다. 실행이 하나뿐이면 드문 일이지만,
     * 손 트리거는 크론 트리거의 겹침 방지 밖이라 스케줄 실행과 나란히 돌 수 있다.
     */
    private record Snapshot(LocalDateTime asOf, long total, long blocked, long unexplained,
            int blockedCoupons) {
    }

    /** {@code null} 이면 아직 모른다. */
    private final AtomicReference<Snapshot> latest = new AtomicReference<>();

    public ExpireMetrics(MeterRegistry registry) {
        gauge(registry, "cy_expire_pending", Snapshot::total,
                "기한이 지났는데 아직 ISSUED 인 발급건");
        gauge(registry, "cy_expire_blocked_pending", Snapshot::blocked,
                "그중 재고가 어긋나 건너뛴 회차의 몫");
        gauge(registry, "cy_expire_unexplained_pending", Snapshot::unexplained,
                "배치가 처리했어야 하는데 안 된 몫. 알림이 보는 값");
        gauge(registry, "cy_expire_blocked_coupons", s -> s.blockedCoupons(),
                "재고가 어긋나 이번 실행이 건너뛴 회차 수");
    }

    /**
     * 실행이 끝난 뒤 한 번 부른다.
     *
     * <p><b>더 오래된 {@code asOf} 의 결과는 안 받는다.</b> 밀린 만료를 따라잡으려고 과거
     * {@code asOf} 로 손 트리거를 치는 것이 이 저장소가 권하는 운영 절차인데, 그 실행은
     * 창이 좁아 <b>더 작은 값</b>을 낸다. 그것이 방금 끝난 주기 실행의 값을 덮으면 관제는
     * <i>"밀린 것이 없다"</i> 를 보고, 누락 알림의 {@code for} 타이머가 리셋된다.
     */
    public void record(LocalDateTime asOf, PendingExpiration pending, int blockedCouponCount) {
        Snapshot fresh = new Snapshot(asOf, pending.total(), pending.blocked(),
                pending.unexplained(), blockedCouponCount);
        // accumulateAndGet 은 (현재값, 새값) 순으로 넘긴다. 뒤집어 쓰면 첫 호출에서
        // 현재값이 null 이라 NPE 가 나고, afterJob 의 catch 가 그것을 삼켜
        // **모든 실행이 조용히 "모름" 이 된다** — 실제로 그렇게 만들었고 테스트가 잡았다.
        latest.accumulateAndGet(fresh,
                (current, next) -> current == null || !current.asOf().isAfter(next.asOf())
                        ? next : current);
    }

    /**
     * <b>세지 못했다는 것을 그대로 내보낸다.</b> 세다가 실패했는데 직전 실행 값을 들고 있으면
     * 관제는 그것을 이번 실행의 결과로 읽는다. 0 을 내면 더 나쁘다 —
     * <i>"밀린 것이 없다"</i> 가 되어 누락 알림이 영원히 조용해진다.
     *
     * <p><b>{@code asOf} 를 아는 실행은 순서를 지킨다.</b> {@link #record} 와 같은 규칙이다 —
     * 더 최신 {@code asOf} 의 결과가 이미 있으면 이 실패가 그것을 못 지운다. 안 그러면
     * 과거 {@code asOf} 로 친 손 트리거가 실패하는 것만으로 <b>방금 끝난 주기의 멀쩡한 값을
     * 지워</b> 그 사이 누락 감시가 꺼진다. {@code record} 에만 순서를 두고 여기 안 두면
     * 같은 우회로가 열린다.
     *
     * <p>이 상태를 감시하는 것은 {@code ExpireMetricsUnknown} 이다. {@code NaN} 은 시리즈로
     * 존재하되 {@code > 0} 비교가 거짓이라 <b>다른 알림이 전부 조용해지기 때문</b>이다.
     */
    public void markUnknown(LocalDateTime asOf) {
        latest.accumulateAndGet(null,
                (current, ignored) -> current == null || !current.asOf().isAfter(asOf)
                        ? null : current);
    }

    /**
     * <b>{@code asOf} 조차 못 믿는 실행.</b> 파라미터가 없거나 미래라 순서를 따질 근거가
     * 없으므로 무조건 지운다 — 그 값으로 센 수는 이 데이터셋의 수가 아니다.
     */
    public void markUnknown() {
        latest.set(null);
    }

    private void gauge(MeterRegistry registry, String name, ToDoubleFunction<Snapshot> field,
            String description) {
        Gauge.builder(name, latest, holder -> {
                    Snapshot snapshot = holder.get();
                    return snapshot == null ? UNKNOWN : field.applyAsDouble(snapshot);
                })
                .description(description)
                .register(registry);
    }
}
