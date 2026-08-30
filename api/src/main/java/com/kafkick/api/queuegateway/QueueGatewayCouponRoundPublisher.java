package com.kafkick.api.queuegateway;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundCatalog;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataReader;
import com.kafkick.core.admin.stock.AdminStockResolver;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.queuegateway.QueueGatewayCouponRoundState;
import com.kafkick.core.queuegateway.QueueGatewayStatePort;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;

/** DB 회차와 버전별 권위 재고를 외부 게이트웨이용 Redis 상태로 주기적으로 변환합니다. */
public final class QueueGatewayCouponRoundPublisher {

    private static final Logger log = LoggerFactory.getLogger(QueueGatewayCouponRoundPublisher.class);

    private final AdminCouponRoundDataReader couponRoundDataReader;
    private final AdminStockResolver stockResolver;
    private final RuntimeConfigStore runtimeConfigStore;
    private final QueueGatewayStatePort statePort;
    private final Clock clock;

    public QueueGatewayCouponRoundPublisher(
            AdminCouponRoundDataReader couponRoundDataReader,
            AdminStockResolver stockResolver,
            RuntimeConfigStore runtimeConfigStore,
            QueueGatewayStatePort statePort,
            Clock clock
    ) {
        this.couponRoundDataReader = Objects.requireNonNull(couponRoundDataReader, "couponRoundDataReader");
        this.stockResolver = Objects.requireNonNull(stockResolver, "stockResolver");
        this.runtimeConfigStore = Objects.requireNonNull(runtimeConfigStore, "runtimeConfigStore");
        this.statePort = Objects.requireNonNull(statePort, "statePort");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** 한 DB 스냅샷의 OPEN 회차만 골라 정책과 버전별 권위 재고를 함께 반영합니다. */
    @Scheduled(fixedDelayString = "${queue.gateway.publisher.coupon-round-interval:5s}")
    public void publishCouponRounds() {
        Instant snapshotAt = clock.instant();
        try {
            AdminCouponRoundCatalog catalog = couponRoundDataReader.loadCatalog(snapshotAt);
            if (catalog.status() != SourceStatus.VALID) {
                log.warn("쿠폰 회차 카탈로그가 {} 상태라 이전 게이트웨이 스냅샷을 보존합니다.",
                        catalog.status());
                return;
            }
            AdminCouponRoundCatalog resolved = stockResolver.resolve(catalog, snapshotAt);
            RuntimeConfigSnapshot runtimeConfig = runtimeConfigStore.get();
            List<QueueGatewayCouponRoundState> openRounds = resolved.couponRounds().stream()
                    .filter(round -> round.status() == CouponRoundStatus.OPEN)
                    .map(QueueGatewayCouponRoundPublisher::toGatewayState)
                    .toList();
            statePort.publishCouponRounds(openRounds, runtimeConfig.queueMode());
        } catch (RuntimeException exception) {
            // 읽기 실패 때 빈 목록을 쓰면 정상 회차까지 종료 처리된다. 이전 스냅샷 보존이 안전하다.
            log.warn("대기열 게이트웨이 쿠폰 회차 상태 공급에 실패해 이전 스냅샷을 보존합니다.", exception);
        }
    }

    private static QueueGatewayCouponRoundState toGatewayState(
            AdminCouponRoundCatalog.CouponRoundData round
    ) {
        CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock = round.stock();
        if (!stock.status().carriesValue()) {
            return new QueueGatewayCouponRoundState(round.couponId(), null, stock.status(), null);
        }
        long remaining = stock.value().totalQuantity() - stock.value().activeCount();
        return new QueueGatewayCouponRoundState(
                round.couponId(), remaining, stock.status(), stock.observedAt());
    }
}
