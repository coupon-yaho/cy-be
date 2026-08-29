// 회차를 open_at 에 열고 close_at 에 닫습니다.
package com.kafkick.batch.schedule;

import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.port.CouponRoundTransitionRepository;
import com.kafkick.batch.config.CouponRoundMetrics;
import com.kafkick.core.support.TimeProvider;

/**
 * <b>Spring Batch 잡이 아니다.</b> 잡으로 만들 조건 셋 — 청크 재시작 · 실행 이력이 판정 근거 ·
 * 파라미터 재실행 증명 — 이 하나도 해당하지 않는다. 그리고 <b>1분 주기로 배치 메타를 쓰면
 * 하루 1,440 인스턴스</b>다. CY-436 이 방금 그 축을 정리했다({@code BATCH_*} 보존 삭제).
 *
 * <p><b>재고 소진으로 닫는 것은 여기가 아니다.</b> {@code api} 의 발급 경로가 그 자리에서
 * 한다. 그래서 선착순 쿠폰에서 <b>마감의 주된 사유가 이 스케줄러를 통과하지 않는다</b> —
 * 여기 오는 것은 소진 안 된 회차의 {@code close_at} 도달뿐이다.
 *
 * <p><b>관측 축이 "돌았나" 가 아니다.</b> 배치 메타를 안 남기므로 마지막 성공 시각이 없다.
 * 대신 {@code CouponRoundPendingRefresher} 가 <i>"열려야 하는데 안 열린 회차가 몇인가"</i> 를
 * DB 에서 되읽고, 알림은 그 게이지에 걸린다 — CY-392 가 {@code BatchJobNotRunning}
 * (증분 창의 델타가 0인지 보는 방식)을 없앤 이유와 같다. <b>실행 여부가 아니라 결과를 본다.</b>
 *
 * <p><b>{@code batch.scheduling.enabled} 하나로 멈춘다.</b> 만료·정리와 같은 스위치다.
 * 끈 채로 배치를 띄우면 위 게이지가 올라가 알림이 뜬다 — 그건 오탐이 아니라 사실이다
 * (회차가 안 열리고 있다). 부하 테스트에서는 컨테이너를 아예 안 띄우므로 게이지가 없어지고,
 * {@code absent()} 알림이 {@code up{job="cy-batch"}} 로 그 구간을 걸러 낸다.
 */
@Component
@ConditionalOnProperty(name = "batch.scheduling.enabled", havingValue = "true",
        matchIfMissing = false)
public class CouponRoundScheduler {

    /**
     * <b>{@code @Scheduled} 는 컴파일 상수만 받는다.</b> 그래서 리터럴이 여기 있고,
     * 형제 스케줄러 둘이 같은 모양이다.
     */
    static final String CRON = "${batch.schedule.coupon-open-cron:0 * * * * *}";

    /** 만료·정리와 같은 좌표계를 봐야 한다. 이유는 {@link ExpireScheduler#ZONE} 에 적었다. */
    static final String ZONE = "${batch.schedule.zone:UTC}";

    private static final Logger log = LoggerFactory.getLogger(CouponRoundScheduler.class);

    private final CouponRoundTransitionRepository rounds;
    private final TimeProvider timeProvider;

    /**
     * <b>카운터는 별도 빈이 소유한다.</b> 이 클래스는 조건부라, 카운터를 여기 두면 스케줄러를
     * 끈 배치에서 시리즈가 사라져 runbook 의 감별({@code increase(ticks[5m]) == 0})을
     * 평가할 수조차 없다. 근거는 {@link CouponRoundMetrics} 에 적었다.
     */
    private final CouponRoundMetrics metrics;

    /** 축("열기"·"닫기")별 연속 실패 수. 첫 실패만 스택을 남기는 데 쓴다. */
    private final java.util.Map<String, java.util.concurrent.atomic.AtomicLong>
            selectFailureStreaks = new java.util.concurrent.ConcurrentHashMap<>();

    public CouponRoundScheduler(CouponRoundTransitionRepository rounds, TimeProvider timeProvider,
            CouponRoundMetrics metrics, @Value(CRON) String couponOpenCron) {
        if (Scheduled.CRON_DISABLED.equals(couponOpenCron)) {
            // **끄는 수단은 하나여야 한다.** 형제 둘은 "-" 를 주면 asOf 를 만들 근거가
            // 사라져서 거절하는데, 이쪽은 그 축이 없다 — 대신 다른 이유로 거절한다.
            //
            // "-" 로 끄면 트리거만 죽고 되읽기는 그대로 돈다(그 빈은 조건부가 아니다).
            // 그러면 CouponRoundsNotOpening 이 5분 뒤부터 **영원히** 뜨는데, 그 runbook 이
            // 가리키는 자리(coupon-open-cron)에는 "-" 가 앉아 있어 읽는 사람이
            // "설정은 되어 있는데 왜 안 도나" 에서 막힌다. 끈 것을 알림에 말해 줄 수단이 없다.
            throw new IllegalArgumentException(
                    "회차 상태 전이를 끄려면 batch.scheduling.enabled=false 를 쓰십시오. "
                            + "batch.schedule.coupon-open-cron 의 \"-\" 는 트리거만 끄고 "
                            + "CouponRoundsNotOpening 은 그대로 울립니다.");
        }
        this.rounds = rounds;
        this.timeProvider = timeProvider;
        this.metrics = metrics;
    }

    /**
     * <b>예외를 밖으로 던지지 않는다.</b> {@code @Scheduled} 에서 예외가 나가면 스프링이 로그만
     * 남기고 다음 주기를 잡는데, 그러면 <b>조용히 안 도는 상태</b>가 된다. 형제 둘이 같은 이유로
     * 같은 모양이다.
     *
     * <p><b>여는 축과 닫는 축을 서로 독립으로 둔다.</b> 한때 대상 조회를 {@code catch} 밖에
     * 뒀는데, 그러면 <b>여는 조회가 실패한 tick 은 닫기를 시도조차 안 했다</b> — 두 전이가
     * 서로 독립이라는 이 클래스의 전제를 <i>회차 단위로는 지키고 전이 종류 단위로는 깨는</i>
     * 모양이었다. 커넥션 풀이 마르는 순간이 정확히 그 경로다.
     *
     * <p><b>시각을 한 번만 읽는다.</b> 여는 대상과 닫는 대상을 다른 시각으로 고르면
     * 같은 tick 안에서 경계를 넘은 회차가 열리고 바로 닫힐 수 있다. 그 시각을 여는 가드에도
     * 넘긴다 — 조회와 갱신이 같은 창을 봐야 한다.
     *
     * <p><b>한 주기를 끝냈다는 것을 카운터로 낸다.</b> 이 스케줄러는 배치 메타를 안 남기므로
     * <i>"돌았나"</i> 를 알 수단이 로그뿐인데, 정상 주기는 로그도 안 남긴다(전이가 0건이면
     * 조용하다) — 그래서 <b>정상과 "아예 안 돎" 이 로그에서 같다.</b> 알림 축은 여전히 결과를
     * 보지만, runbook 이 <i>"스케줄러 쪽인지"</i> 를 감별할 수단이 하나는 있어야 한다.
     */
    @Scheduled(cron = CRON, zone = ZONE)
    public void transitionRounds() {
        LocalDateTime now = timeProvider.now();
        int opened = openDue(now);
        int closed = closeDue(now);
        metrics.tickCompleted();
        if (opened > 0 || closed > 0) {
            log.info("회차 상태를 전이했습니다. 열림={} 닫힘={} 기준시각={}", opened, closed, now);
        }
    }

    private int openDue(LocalDateTime now) {
        List<Long> candidates = candidates(() -> rounds.roundsToOpen(now), "열기", now);
        return transition(candidates, couponId -> rounds.open(couponId, now), "열기");
    }

    private int closeDue(LocalDateTime now) {
        List<Long> candidates = candidates(() -> rounds.roundsToClose(now), "닫기", now);
        return transition(candidates, couponId -> rounds.close(couponId, now), "닫기");
    }

    /**
     * <b>조회 실패는 그 축만 건너뛴다.</b> 빈 목록을 돌려주면 호출자는 <i>"할 일이 없었다"</i>
     * 와 구분할 수 없는데, 그 구분은 <b>게이지가 진다</b> — 안 바뀐 회차는 다음 주기에도
     * 대기로 남아 있다. 여기서 예외를 다시 던지면 반대 축이 함께 죽는다.
     */
    private List<Long> candidates(Supplier<List<Long>> select, String what, LocalDateTime now) {
        try {
            List<Long> candidates = select.get();
            selectFailureStreak(what).set(0);
            return candidates;
        } catch (Exception e) {
            // **전이 실패와 다른 카운터로 센다.** 이걸 안 세면 조회가 매 tick 죽는 동안
            // ticks 는 오르고 transition_failures 는 0 이라, runbook 이 "돌고 있으면 전이
            // 실패를 보라" 로 보낸 사람이 막다른 길에 선다.
            metrics.selectFailed();
            // **스택은 첫 실패만.** 크론이 1분이고 축이 둘이라 억제가 없으면 하루 최대
            // 2,880줄 × 스택이고, 그것이 이 티켓의 runbook 다섯이 사람을 보내는 그 로그를
            // 밀어낸다. 형제 되읽기가 같은 판단을 했다 — 하루 1회인 만료·정리가 스택을
            // 그냥 찍는 것은 주기가 달라서지 다른 기준이 아니다.
            if (selectFailureStreak(what).getAndIncrement() == 0) {
                log.error("{} 대상을 고르지 못해 이번 주기의 {}를 건너뜁니다. 기준시각={}",
                        what, what, now, e);
            } else {
                log.error("{} 대상을 고르지 못해 이번 주기의 {}를 건너뜁니다. 기준시각={} 원인={}",
                        what, what, now, e.toString());
            }
            return List.of();
        }
    }

    /**
     * <b>{@code false} 를 오류로 세지 않는다.</b> 그 사이 누가 상태를 바꿨다는 뜻이고,
     * 닫기 쪽에서는 <b>정상 경로</b>다 — 재고 소진으로 발급 경로가 먼저 닫으면 여기는 0행이다.
     * 그것을 실패로 로그하면 <b>가장 흔한 마감이 매번 오류로 보고된다.</b>
     *
     * <p>⚠️ <b>그 발급 경로는 아직 없다</b>({@code api} 에 발급 코드가 0줄이다). 그 티켓이
     * 들어오기 전까지 닫기 쪽 {@code false} 는 <b>다른 원인</b>이고, 그때는 이 판단을 다시 봐야
     * 한다 — 지금은 debug 로 낮춰 두었다.
     *
     * <p><b>스택트레이스는 tick 당 한 번이다.</b> 반복 실패는 대상 전부에서 <b>같은 원인</b>으로
     * 난다(락 대기 · 권한 · 커넥션). 회차마다 스택을 찍으면 한 tick 에 대상 수만큼 쌓이고
     * 1분마다 반복되어, 이 티켓이 만든 runbook 다섯(NotOpening · NotClosing · SelectFailing · MetricsStale · MetricsUnknown)이 사람을 보내는 그 로그를 밀어낸다.
     * 형제 되읽기가 같은 이유로 같은 판단을 했다.
     */
    private int transition(List<Long> candidates, RoundTransition transition, String what) {
        int changed = 0;
        int failed = 0;
        Exception representative = null;
        for (long couponId : candidates) {
            try {
                if (transition.apply(couponId)) {
                    changed++;
                } else {
                    log.debug("회차 {} 는 이미 다른 상태입니다. {}를 건너뜁니다.", couponId, what);
                }
            } catch (Exception e) {
                // 한 회차의 실패가 나머지를 멈추지 않는다. 안 바뀐 것은 게이지가 진다.
                failed++;
                representative = e;
                log.error("회차 {} {}에 실패했습니다. 나머지는 계속합니다. 원인={}",
                        couponId, what, e.toString());
            }
        }
        if (failed > 0) {
            metrics.transitionsFailed(failed);
            log.error("{} {}건이 실패했습니다(대상 {}건). 대표 원인은 다음과 같습니다.",
                    what, failed, candidates.size(), representative);
        }
        return changed;
    }

    /**
     * <b>축마다 따로 센다.</b> 하나로 묶으면 열기가 계속 실패하는 동안 닫기의 첫 실패가
     * 스택 없이 접힌다 — 원인이 다를 수 있는데 첫 발생의 단서를 잃는다.
     */
    private java.util.concurrent.atomic.AtomicLong selectFailureStreak(String what) {
        return selectFailureStreaks.computeIfAbsent(
                what, ignored -> new java.util.concurrent.atomic.AtomicLong());
    }

    @FunctionalInterface
    private interface RoundTransition {
        boolean apply(long couponId);
    }
}
