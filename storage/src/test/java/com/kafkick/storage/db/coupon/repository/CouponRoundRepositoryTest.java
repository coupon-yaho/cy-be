// 실제 MySQL에서 쿠폰 회차 스냅샷과 최초 재고의 원자 저장 및 중복 방어를 검증합니다.
package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponRoundAlreadyExistsException;
import com.kafkick.core.coupon.exception.CouponRoundPersistenceException;
import com.kafkick.core.coupon.service.CouponRoundCreationService;
import com.kafkick.storage.db.RepositoryTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RepositoryTest
@Import({
        CouponRoundRepositoryImpl.class,
        CouponRoundCreationService.class,
        CouponTemplateRepositoryImpl.class,
        CouponRoundRepositoryTest.AuditTestConfig.class
})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class CouponRoundRepositoryTest {

    private static final long CONCURRENCY_TIMEOUT_SECONDS = 10;
    private static final Instant AUDIT_CREATED_AT =
            Instant.parse("2030-01-01T00:00:00Z");

    @Autowired
    private CouponRoundCreationService couponRoundCreationService;

    @Autowired
    private CouponTemplateRepositoryImpl couponTemplateRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void resetData() {
        jdbcTemplate.update("DELETE FROM coupon_stocks");
        jdbcTemplate.update("DELETE FROM coupons");
        jdbcTemplate.update("DELETE FROM coupon_templates");
        jdbcTemplate.update("DELETE FROM brands");
        jdbcTemplate.update(
                "INSERT INTO brands (id, name, category) VALUES (?, ?, ?)",
                1L,
                "테스트 브랜드",
                "카페"
        );
    }

    @Test
    @DisplayName("회차 생성 기준 시각과 감사 시각을 분리해 최초 재고와 함께 저장한다")
    void saveCouponRoundWithInitialStock() {
        CouponTemplate template = saveTemplate();
        Instant generatedAt = Instant.parse("2020-01-01T00:00:00Z");
        CouponRound couponRound = scheduledRound(template, generatedAt);
        CouponStock initialStock = CouponStock.initialize(
                template.stockPerOccurrence(),
                generatedAt
        );

        CouponRound savedRound = couponRoundCreationService
                .create(couponRound, initialStock);

        Map<String, Object> roundRow = jdbcTemplate.queryForMap(
                """
                SELECT template_id, brand_id, name, policy_type,
                       discount_rate, max_discount_amount, discount_amount,
                       valid_days, eligible_grades_mask,
                       open_at, close_at, status,
                       generated_at, created_at
                FROM coupons
                WHERE id = ?
                """,
                savedRound.id()
        );
        Map<String, Object> stockRow = jdbcTemplate.queryForMap(
                """
                SELECT coupon_id, total_quantity, active_count, updated_at
                FROM coupon_stocks
                WHERE coupon_id = ?
                """,
                savedRound.id()
        );

        assertThat(savedRound.id()).isPositive();
        assertThat(savedRound.status()).isEqualTo(CouponRoundStatus.SCHEDULED);
        assertThat(savedRound.generatedAt()).isEqualTo(generatedAt);
        assertThat(((Number) roundRow.get("template_id")).longValue())
                .isEqualTo(template.id());
        assertThat(((Number) roundRow.get("brand_id")).longValue())
                .isEqualTo(template.brandId());
        assertThat(roundRow.get("name")).isEqualTo(template.name());
        assertThat(roundRow.get("policy_type"))
                .isEqualTo(CouponPolicyType.FIXED_AMOUNT.name());
        assertThat(roundRow.get("discount_rate")).isNull();
        assertThat(roundRow.get("max_discount_amount")).isNull();
        assertThat(((Number) roundRow.get("discount_amount")).intValue())
                .isEqualTo(5_000);
        assertThat(((Number) roundRow.get("valid_days")).intValue())
                .isEqualTo(7);
        assertThat(((Number) roundRow.get("eligible_grades_mask")).intValue())
                .isEqualTo(12);
        assertThat(roundRow.get("open_at"))
                .isEqualTo(LocalDateTime.of(2026, 9, 8, 5, 0));
        assertThat(roundRow.get("close_at"))
                .isEqualTo(LocalDateTime.of(2026, 9, 8, 7, 0));
        assertThat(roundRow.get("status"))
                .isEqualTo(CouponRoundStatus.SCHEDULED.name());
        assertThat(roundRow.get("generated_at"))
                .isEqualTo(LocalDateTime.of(2020, 1, 1, 0, 0));
        assertThat(roundRow.get("created_at"))
                .isEqualTo(LocalDateTime.of(2030, 1, 1, 0, 0));
        assertThat(((Number) stockRow.get("coupon_id")).longValue())
                .isEqualTo(savedRound.id());
        assertThat(((Number) stockRow.get("total_quantity")).intValue())
                .isEqualTo(10_000);
        assertThat(((Number) stockRow.get("active_count")).intValue())
                .isZero();
        assertThat(stockRow.get("updated_at"))
                .isEqualTo(LocalDateTime.of(2020, 1, 1, 0, 0));
    }

    @Test
    @DisplayName("회차 생성 대상은 활성 상태인 지원 할인 정책만 조회한다")
    void findOnlyActiveTemplatesByIdAsc() {
        saveTemplate();
        jdbcTemplate.update(
                """
                INSERT INTO coupon_templates (
                    id, brand_id, name, policy_type,
                    discount_amount, valid_days, nth_week, day_of_week,
                    start_time, duration_hours, stock_per_occurrence,
                    eligible_grades_mask, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                101L,
                1L,
                "비활성 템플릿",
                CouponPolicyType.FIXED_AMOUNT.name(),
                3_000,
                7,
                1,
                CouponDayOfWeek.MON.name(),
                LocalTime.of(10, 0),
                2,
                100,
                4,
                false
        );
        jdbcTemplate.update(
                """
                INSERT INTO coupon_templates (
                    id, brand_id, name, policy_type,
                    data_grant_mb, valid_days, nth_week, day_of_week,
                    start_time, duration_hours, stock_per_occurrence,
                    eligible_grades_mask, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                102L,
                1L,
                "지원 범위 밖 레거시 데이터 쿠폰",
                "DATA_GRANT",
                1_024,
                7,
                1,
                CouponDayOfWeek.MON.name(),
                LocalTime.of(10, 0),
                2,
                100,
                4,
                true
        );

        assertThat(couponTemplateRepository.findAllActiveByIdAsc())
                .extracting(CouponTemplate::id)
                .containsExactly(100L);
    }

    @Test
    @DisplayName("존재하지 않는 템플릿이면 회차와 재고가 모두 롤백된다")
    void rollbackRoundAndStockTogether() {
        CouponTemplate unsavedTemplate = template(999L);
        Instant generatedAt = Instant.parse("2026-08-18T00:00:00Z");

        assertThatThrownBy(() -> couponRoundCreationService.create(
                scheduledRound(unsavedTemplate, generatedAt),
                CouponStock.initialize(10_000, generatedAt)
        )).isInstanceOf(CouponRoundPersistenceException.class);

        assertThat(countRows("coupons")).isZero();
        assertThat(countRows("coupon_stocks")).isZero();
    }

    @Test
    @DisplayName("DB 제약이 active_count의 재고 범위 초과를 거부한다")
    void rejectStockInvariantViolationAtDatabase() {
        CouponTemplate template = saveTemplate();
        Instant generatedAt = Instant.parse("2026-08-18T00:00:00Z");
        CouponRound savedRound = couponRoundCreationService.create(
                scheduledRound(template, generatedAt),
                CouponStock.initialize(10_000, generatedAt)
        );

        assertThatThrownBy(() -> jdbcTemplate.update(
                """
                UPDATE coupon_stocks
                SET active_count = total_quantity + 1
                WHERE coupon_id = ?
                """,
                savedRound.id()
        )).isInstanceOf(DataAccessException.class);

        assertThat(jdbcTemplate.queryForObject(
                "SELECT active_count FROM coupon_stocks WHERE coupon_id = ?",
                Integer.class,
                savedRound.id()
        )).isZero();
    }

    @Test
    @DisplayName("DB 제약이 종료 시각이 시작 시각보다 빠른 회차를 거부한다")
    void rejectInvalidRoundTimeRangeAtDatabase() {
        CouponTemplate template = saveTemplate();

        assertThatThrownBy(() -> jdbcTemplate.update(
                """
                INSERT INTO coupons (
                    template_id, brand_id, name, policy_type,
                    discount_amount, valid_days, eligible_grades_mask,
                    open_at, close_at, status, generated_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                template.id(),
                template.brandId(),
                template.name(),
                template.policyType().name(),
                template.discountAmount(),
                template.validDays(),
                template.eligibleGradesMask(),
                LocalDateTime.of(2026, 9, 8, 7, 0),
                LocalDateTime.of(2026, 9, 8, 5, 0),
                CouponRoundStatus.SCHEDULED.name(),
                LocalDateTime.of(2026, 8, 18, 0, 0),
                LocalDateTime.of(2026, 8, 18, 0, 0)
        )).isInstanceOf(DataAccessException.class);

        assertThat(countRows("coupons")).isZero();
    }

    @Test
    @DisplayName("동일 회차를 동시에 두 번 생성해도 DB 유니크 제약으로 한 건만 남긴다")
    void createSameCouponRoundConcurrentlyOnce() throws Exception {
        CouponTemplate template = saveTemplate();
        Instant generatedAt = Instant.parse("2026-08-18T00:00:00Z");
        CouponRound couponRound = scheduledRound(template, generatedAt);
        CouponStock initialStock = CouponStock.initialize(10_000, generatedAt);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            Future<Class<?>> firstResult = executorService.submit(() ->
                    createConcurrently(
                            couponRound,
                            initialStock,
                            ready,
                            start
                    )
            );
            Future<Class<?>> secondResult = executorService.submit(() ->
                    createConcurrently(
                            couponRound,
                            initialStock,
                            ready,
                            start
                    )
            );

            assertThat(ready.await(
                    CONCURRENCY_TIMEOUT_SECONDS,
                    TimeUnit.SECONDS
            )).isTrue();
            start.countDown();

            assertThat(List.of(
                    firstResult.get(
                            CONCURRENCY_TIMEOUT_SECONDS,
                            TimeUnit.SECONDS
                    ),
                    secondResult.get(
                            CONCURRENCY_TIMEOUT_SECONDS,
                            TimeUnit.SECONDS
                    )
            ))
                    .containsExactlyInAnyOrder(
                            CouponRound.class,
                            CouponRoundAlreadyExistsException.class
                    );
        } finally {
            start.countDown();
            executorService.shutdownNow();
            assertThat(executorService.awaitTermination(
                    CONCURRENCY_TIMEOUT_SECONDS,
                    TimeUnit.SECONDS
            )).isTrue();
        }

        assertThat(countRows("coupons")).isEqualTo(1);
        assertThat(countRows("coupon_stocks")).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT active_count FROM coupon_stocks",
                Integer.class
        )).isZero();
    }

    private Class<?> createConcurrently(
            CouponRound couponRound,
            CouponStock initialStock,
            CountDownLatch ready,
            CountDownLatch start
    ) throws InterruptedException {
        ready.countDown();
        if (!start.await(
                CONCURRENCY_TIMEOUT_SECONDS,
                TimeUnit.SECONDS
        )) {
            throw new IllegalStateException(
                    "동시성 테스트 시작 신호를 받지 못했습니다."
            );
        }
        try {
            couponRoundCreationService.create(
                    couponRound,
                    initialStock
            );
            return CouponRound.class;
        } catch (CouponRoundAlreadyExistsException exception) {
            return CouponRoundAlreadyExistsException.class;
        }
    }

    private CouponTemplate saveTemplate() {
        jdbcTemplate.update(
                """
                INSERT INTO coupon_templates (
                    id, brand_id, name, policy_type,
                    discount_rate, max_discount_amount, discount_amount,
                    valid_days, nth_week, day_of_week, start_time,
                    duration_hours, stock_per_occurrence,
                    eligible_grades_mask, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                100L,
                1L,
                "골드 VIP 5천원 할인",
                CouponPolicyType.FIXED_AMOUNT.name(),
                null,
                null,
                5_000,
                7,
                2,
                CouponDayOfWeek.TUE.name(),
                LocalTime.of(14, 0),
                2,
                10_000,
                12,
                true
        );
        return template(100L);
    }

    private CouponTemplate template(Long id) {
        return CouponTemplate.restore(
                id,
                1L,
                "골드 VIP 5천원 할인",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                2,
                CouponDayOfWeek.TUE,
                LocalTime.of(14, 0),
                2,
                10_000,
                Set.of(MembershipGrade.GOLD, MembershipGrade.VIP),
                true
        );
    }

    private CouponRound scheduledRound(
            CouponTemplate template,
            Instant generatedAt
    ) {
        return CouponRound.schedule(
                template,
                Instant.parse("2026-09-08T05:00:00Z"),
                generatedAt
        );
    }

    private int countRows(String tableName) {
        String query = switch (tableName) {
            case "coupons" -> "SELECT COUNT(*) FROM coupons";
            case "coupon_stocks" ->
                    "SELECT COUNT(*) FROM coupon_stocks";
            default -> throw new IllegalArgumentException(
                    "허용되지 않은 테스트 테이블입니다."
            );
        };

        return jdbcTemplate.queryForObject(
                query,
                Integer.class
        );
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableJpaAuditing(
            dateTimeProviderRef = "couponRoundTestDateTimeProvider"
    )
    static class AuditTestConfig {

        @Bean
        DateTimeProvider couponRoundTestDateTimeProvider() {
            return () -> Optional.of(AUDIT_CREATED_AT);
        }
    }
}
