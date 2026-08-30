package com.kafkick.api.observation;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import com.kafkick.ApiApplication;
import com.kafkick.core.observation.CouponRoundLifecycleRecorder;
import com.kafkick.core.observation.ClosedCouponRound;
import com.kafkick.core.observation.ClosedCouponRoundRecoverySource;
import com.kafkick.storage.db.MySqlContainerConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = ApiApplication.class, properties = {
        "observation.datasource.enabled=true",
        "coupon-round.lifecycle.redis.subscriber-enabled=false",
        "coupon.idempotency.wait-timeout=1s",
        "coupon.idempotency.poll-interval=50ms",
        "coupon.idempotency.stale-after=30s",
        "coupon.round-generation.schedule-zone=Asia/Seoul",
        "coupon.calendar.max-query-range-days=366"
})
@Import(MySqlContainerConfig.class)
class CouponRoundLifecycleStartupRecoveryMySqlIntegrationTest {

    private static final Instant ANCHOR = Instant.now();

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Autowired
    private ClosedCouponRoundRecoverySource source;

    @Autowired
    private CouponRoundLifecycleStartupRecovery recovery;

    @MockitoBean
    private CouponRoundLifecycleRecorder recorder;

    @BeforeEach
    void insertCouponRoundPopulation() {
        jdbc.update("DELETE FROM coupon_stocks");
        jdbc.update("DELETE FROM coupons");
        jdbc.update("DELETE FROM coupon_templates");
        jdbc.update("DELETE FROM brands");
        jdbc.update("INSERT INTO brands(id, name, category) VALUES (1, 'brand', 'CAFE')");
        jdbc.update("""
                INSERT INTO coupon_templates(
                    id, brand_id, name, policy_type, valid_days,
                    nth_week, day_of_week, start_time, duration_hours,
                    stock_per_occurrence, eligible_grades_mask, active,
                    created_at, updated_at
                ) VALUES (1, 1, 'template', 'FIXED_AMOUNT', 30,
                          1, 'MON', '10:00:00', 1,
                          100, 1, true, ?, ?)
                """, timestamp(ANCHOR.minus(Duration.ofDays(3))),
                timestamp(ANCHOR.minus(Duration.ofDays(3))));

        List<Object[]> couponRounds = new ArrayList<>();
        for (long id = 1; id <= 1_002; id++) {
            couponRounds.add(couponRound(
                    id,
                    "CLOSED",
                    ANCHOR.minus(Duration.ofHours(12)).plusSeconds(id)
            ));
        }
        couponRounds.add(couponRound(2_001L, "CLOSED",
                ANCHOR.minus(Duration.ofDays(2))));
        couponRounds.add(couponRound(2_002L, "CLOSED",
                ANCHOR.plus(Duration.ofDays(1))));
        couponRounds.add(couponRound(2_003L, "OPEN",
                ANCHOR.minusSeconds(10)));
        couponRounds.add(couponRound(2_004L, "SCHEDULED",
                ANCHOR.minusSeconds(10)));
        jdbc.batchUpdate("""
                INSERT INTO coupons(
                    id, template_id, brand_id, name, policy_type,
                    valid_days, eligible_grades_mask, open_at, close_at,
                    status, generated_at, created_at
                ) VALUES (?, 1, 1, ?, 'FIXED_AMOUNT',
                          30, 1, ?, ?, ?, ?, ?)
                """, couponRounds);
        clearInvocations(recorder);
    }

    @Test
    @DisplayName("실제 관측 계정으로 최신 CLOSED 1000건을 고른 뒤 오래된 순으로 회수한다")
    void recoverSelectedPopulationOldestFirst() throws Exception {
        Instant queryNow = Instant.now();
        List<ClosedCouponRound> selected = source.findRecentlyClosed(
                queryNow.minus(Duration.ofDays(1)),
                queryNow,
                1_000
        );

        assertThat(selected).hasSize(1_000);
        assertThat(selected.getFirst().couponId())
                .isEqualTo(1_002L);
        assertThat(selected.getLast().couponId())
                .isEqualTo(3L);

        recovery.run(null);

        ArgumentCaptor<Long> ids = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Instant> closedAt =
                ArgumentCaptor.forClass(Instant.class);
        verify(recorder, times(1_000)).retireCouponRound(
                ids.capture(),
                closedAt.capture()
        );
        assertThat(ids.getAllValues()).containsExactlyElementsOf(
                LongStream.rangeClosed(3L, 1_002L)
                        .boxed()
                        .toList()
        );
        assertThat(closedAt.getAllValues()).isSorted();
    }

    private static Object[] couponRound(
            long id,
            String status,
            Instant closeAt
    ) {
        Instant openAt = ANCHOR.minus(Duration.ofDays(3)).plusSeconds(id);
        return new Object[] {
                id,
                "couponRound-" + id,
                timestamp(openAt),
                timestamp(closeAt),
                status,
                timestamp(openAt.minusSeconds(1)),
                timestamp(openAt.minusSeconds(1))
        };
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.from(instant);
    }
}
