package com.kafkick.storage.db.coupon.repository;

import java.sql.Statement;
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
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupontemplate.domain.CouponDayOfWeek;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponRoundAlreadyExistsException;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.exception.CouponRoundScheduleConflictException;
import com.kafkick.core.coupon.service.CouponRoundCreationService;
import com.kafkick.core.coupon.service.CouponRoundLifecycleService;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.coupontemplate.repository.CouponTemplateRepositoryImpl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// 실제 MySQL에서 회차 스냅샷과 전역 예약 잠금의 원자성을 검증합니다.
@RepositoryTest
@Import({
        CouponRoundRepositoryImpl.class,
        CouponRoundScheduleLockAdapter.class,
        CouponRoundLifecycleAdapter.class,
        CouponRoundCreationService.class,
        CouponRoundLifecycleService.class,
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
    private CouponRoundLifecycleService couponRoundLifecycleService;

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
    @DisplayName("회차 생성 대상은 활성 상태인 템플릿만 조회한다")
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
        assertThat(couponTemplateRepository.findAllActiveByIdAsc())
                .extracting(CouponTemplate::id)
                .containsExactly(100L);
    }

    @Test
    @DisplayName("DB는 DATA_GRANT 정책과 전용 컬럼을 허용하지 않는다")
    void rejectDataGrantPolicyAndRemoveDedicatedColumns() {
        Integer columns = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                  FROM information_schema.columns
                 WHERE table_schema = DATABASE()
                   AND table_name IN ('coupon_templates', 'coupons')
                   AND column_name = 'data_grant_mb'
                """,
                Integer.class
        );
        assertThat(columns).isZero();

        assertThatThrownBy(() -> jdbcTemplate.update(
                """
                INSERT INTO coupon_templates (
                    id, brand_id, name, policy_type,
                    valid_days, nth_week, day_of_week,
                    start_time, duration_hours, stock_per_occurrence,
                    eligible_grades_mask, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                102L,
                1L,
                "지원하지 않는 데이터 쿠폰",
                "DATA_GRANT",
                7,
                1,
                CouponDayOfWeek.MON.name(),
                LocalTime.of(10, 0),
                2,
                100,
                4,
                true
        )).isInstanceOf(DataAccessException.class);
    }

    @Test
    @DisplayName("V17은 미지원 정책이 있으면 영구 DDL 전에 중단한다")
    void stopV17BeforePermanentDdlWhenUnsupportedPolicyExists() {
        jdbcTemplate.execute((ConnectionCallback<Void>) connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TEMPORARY TABLE coupon_templates (
                            policy_type VARCHAR(20) NOT NULL,
                            data_grant_mb INT
                        )
                        """);
                statement.execute("""
                        CREATE TEMPORARY TABLE coupons (
                            policy_type VARCHAR(20) NOT NULL,
                            data_grant_mb INT
                        )
                        """);
                statement.execute("""
                        INSERT INTO coupons (policy_type, data_grant_mb)
                        VALUES ('DATA_GRANT', 1024)
                        """);

                assertThatThrownBy(() -> ScriptUtils.executeSqlScript(
                        connection,
                        new EncodedResource(new ClassPathResource(
                                "db/migration/V17__remove_data_grant_policy.sql"
                        ))
                )).isInstanceOf(DataAccessException.class);

                assertThat(statement.executeQuery(
                        "SELECT data_grant_mb FROM coupon_templates LIMIT 0"
                )).isNotNull();
                assertThat(statement.executeQuery(
                        "SELECT data_grant_mb FROM coupons LIMIT 0"
                )).isNotNull();
            } finally {
                try (Statement cleanup = connection.createStatement()) {
                    cleanup.execute("DROP TEMPORARY TABLE IF EXISTS v17_coupon_policy_guard");
                    cleanup.execute("DROP TEMPORARY TABLE IF EXISTS coupon_templates");
                    cleanup.execute("DROP TEMPORARY TABLE IF EXISTS coupons");
                }
            }
            return null;
        });
    }

    @Test
    @DisplayName("V18은 미지원 회차 정책이 있으면 회차 DDL 전에 중단한다")
    void stopV18BeforeCouponRoundDdlWhenUnsupportedPolicyExists() {
        jdbcTemplate.execute((ConnectionCallback<Void>) connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TEMPORARY TABLE coupons (
                            policy_type VARCHAR(20) NOT NULL,
                            data_grant_mb INT
                        )
                        """);
                statement.execute("""
                        INSERT INTO coupons (policy_type, data_grant_mb)
                        VALUES ('DATA_GRANT', 1024)
                        """);

                assertThatThrownBy(() -> ScriptUtils.executeSqlScript(
                        connection,
                        new EncodedResource(new ClassPathResource(
                                "db/migration/V18__remove_coupon_round_data_grant_policy.sql"
                        ))
                )).isInstanceOf(DataAccessException.class);

                assertThat(statement.executeQuery(
                        "SELECT data_grant_mb FROM coupons LIMIT 0"
                )).isNotNull();
            } finally {
                try (Statement cleanup = connection.createStatement()) {
                    cleanup.execute("DROP TEMPORARY TABLE IF EXISTS v18_coupon_policy_guard");
                    cleanup.execute("DROP TEMPORARY TABLE IF EXISTS coupons");
                }
            }
            return null;
        });
    }

    @Test
    @DisplayName("존재하지 않는 템플릿이면 회차와 재고가 모두 롤백된다")
    void rollbackRoundAndStockTogether() {
        CouponTemplate unsavedTemplate = template(999L);
        Instant generatedAt = Instant.parse("2026-08-18T00:00:00Z");

        assertThatThrownBy(() -> couponRoundCreationService.create(
                scheduledRound(unsavedTemplate, generatedAt),
                CouponStock.initialize(10_000, generatedAt)
        )).isInstanceOf(CouponPersistenceException.class);

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
    @DisplayName("겹치는 회차를 동시에 두 번 예약해도 전역 잠금으로 한 건만 남긴다")
    void createSameCouponRoundConcurrentlyOnce() throws Exception {
        CouponTemplate firstTemplate = saveTemplate();
        jdbcTemplate.update(
                "INSERT INTO brands (id, name, category) VALUES (?, ?, ?)",
                2L,
                "두 번째 테스트 브랜드",
                "카페"
        );
        CouponTemplate secondTemplate = saveTemplate(
                101L,
                2L,
                "다른 브랜드 골드 VIP 5천원 할인"
        );
        Instant generatedAt = Instant.parse("2026-08-18T00:00:00Z");
        CouponRound firstRound = scheduledRound(
                firstTemplate,
                generatedAt
        );
        CouponRound secondRound = scheduledRound(
                secondTemplate,
                generatedAt
        );
        CouponStock initialStock = CouponStock.initialize(10_000, generatedAt);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            Future<Class<?>> firstResult = executorService.submit(() ->
                    createConcurrently(
                            firstRound,
                            initialStock,
                            ready,
                            start
                    )
            );
            Future<Class<?>> secondResult = executorService.submit(() ->
                    createConcurrently(
                            secondRound,
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
                            CouponRoundScheduleConflictException.class
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
        } catch (CouponRoundScheduleConflictException exception) {
            return CouponRoundScheduleConflictException.class;
        }
    }

    private CouponTemplate saveTemplate() {
        return saveTemplate(100L, 1L, "골드 VIP 5천원 할인");
    }

    @Test
    @DisplayName("인접한 예약 회차를 시작·종료 시각에 맞춰 하나씩 연다")
    void synchronizeAdjacentRoundLifecycle() {
        CouponTemplate template = saveTemplate();
        Instant generatedAt = Instant.parse("2026-08-20T00:00:00Z");
        CouponRound first = CouponRound.schedule(
                template,
                Instant.parse("2026-09-08T05:00:00Z"),
                Instant.parse("2026-09-08T06:00:00Z"),
                generatedAt
        );
        CouponRound second = CouponRound.schedule(
                template,
                Instant.parse("2026-09-08T06:00:00Z"),
                Instant.parse("2026-09-08T07:00:00Z"),
                generatedAt
        );
        CouponStock stock = CouponStock.initialize(100, generatedAt);
        CouponRound savedFirst = couponRoundCreationService.create(
                first,
                stock
        );
        CouponRound savedSecond = couponRoundCreationService.create(
                second,
                stock
        );

        couponRoundLifecycleService.synchronize(
                Instant.parse("2026-09-08T05:30:00Z")
        );

        assertThat(statusOf(savedFirst.id()))
                .isEqualTo(CouponRoundStatus.OPEN.name());
        assertThat(statusOf(savedSecond.id()))
                .isEqualTo(CouponRoundStatus.SCHEDULED.name());

        couponRoundLifecycleService.synchronize(
                Instant.parse("2026-09-08T06:00:00Z")
        );

        assertThat(statusOf(savedFirst.id()))
                .isEqualTo(CouponRoundStatus.CLOSED.name());
        assertThat(statusOf(savedSecond.id()))
                .isEqualTo(CouponRoundStatus.OPEN.name());
    }

    private CouponTemplate saveTemplate(
            Long id,
            Long brandId,
            String name
    ) {
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
                id,
                brandId,
                name,
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
        return template(id, brandId, name);
    }

    private CouponTemplate template(Long id) {
        return template(id, 1L, "골드 VIP 5천원 할인");
    }

    private CouponTemplate template(
            Long id,
            Long brandId,
            String name
    ) {
        return CouponTemplate.restore(
                id,
                brandId,
                name,
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

    private String statusOf(Long couponRoundId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM coupons WHERE id = ?",
                String.class,
                couponRoundId
        );
    }

    @TestConfiguration
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
