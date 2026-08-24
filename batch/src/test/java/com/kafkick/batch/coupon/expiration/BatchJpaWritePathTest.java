package com.kafkick.batch.coupon.expiration;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import jakarta.persistence.EntityManagerFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * batch 가 JPA 를 <b>실제로 쓴다</b>는 것을 쓰기까지 태워 확인한다.
 *
 * <p>CY-245 계보가 들어오기 전 batch 는 JPA 를 뺀 채로 돌았고, 그 전제가 사라지면서 자동설정
 * 제외와 auditing 스위치를 함께 걷었다. <b>컨텍스트가 뜨는 것만으로는 그 변경이 옳은지 알 수
 * 없다</b> — auditing 을 끈 채 JPA 만 켜도 기동은 성공하고 쓰기 시점에만 실패하기 때문이다.
 * 그래서 이 테스트는 기동이 아니라 다음을 본다.
 *
 * <ul>
 *   <li>{@code EntityManagerFactory} 와 JPA 리포지토리가 batch 컨텍스트에 실제로 있다
 *   <li>JPA 로 쓴 행에 감사 필드({@code created_at})가 채워진다 — auditing 이 살아 있다
 *   <li>만료 업무를 한 번 돌리면 상태 전이·이력 적재·재고 반납이 DB 에 남는다
 * </ul>
 *
 * <p><b>잡지 못하는 것.</b> 스케줄러가 이 러너를 제 주기에 부르는지는 보지 않는다. 그건
 * {@code CouponExpirationSchedulerTest} 몫이다.
 */
@SpringBootTest(properties = "spring.flyway.enabled=true")
@Import(MySqlContainerConfig.class)
class BatchJpaWritePathTest {

    private static final long COUPON_ROUND_ID = 10L;
    private static final long MEMBER_ID = 1L;
    private static final String CODE = "OBSJPA0000000001";

    @Autowired
    private ApplicationContext context;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private IssuanceRepository issuanceRepository;
    @Autowired
    private CouponExpirationRunner runner;
    @Autowired
    private PlatformTransactionManager transactionManager;

    @BeforeEach
    void seed() {
        jdbcTemplate.update("DELETE FROM issuance_histories");
        jdbcTemplate.update("DELETE FROM issuances");
        jdbcTemplate.update("DELETE FROM coupon_stocks");
        jdbcTemplate.update("DELETE FROM coupons");
        jdbcTemplate.update("DELETE FROM coupon_templates");
        jdbcTemplate.update("DELETE FROM members");
        jdbcTemplate.update("DELETE FROM brands");
        jdbcTemplate.update("DELETE FROM grades");
        saveOpenCouponRound();
    }

    @Test
    @DisplayName("batch 컨텍스트에 EntityManagerFactory 와 JPA 리포지토리가 있다")
    void theBatchContextOwnsTheJpaLayer() {
        assertThat(context.getBeansOfType(EntityManagerFactory.class))
                .as("자동설정 제외를 남겨 두면 storage 의 @EnableJpaRepositories 가 만든"
                        + " 리포지토리가 이 빈을 못 찾아 기동에서 죽는다")
                .isNotEmpty();
        assertThat(context.getBeanNamesForType(
                org.springframework.data.jpa.repository.JpaRepository.class))
                .isNotEmpty();
    }

    @Test
    @DisplayName("JPA 로 쓴 발급 행에 감사 시각이 채워진다 — auditing 을 끄면 여기서 깨진다")
    void auditingFillsCreatedAtOnWrite() {
        Issuance saved = saveInTransaction(expiredIssuance());

        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT created_at, updated_at FROM issuances WHERE id = ?", saved.id());

        assertThat(row.get("created_at"))
                .as("엔티티가 @CreatedDate 와 AuditingEntityListener 를 쓴다."
                        + " storage.jpa.auditing.enabled 를 끄면 기동은 되고 이 값만 빈다")
                .isNotNull();
        assertThat(row.get("updated_at")).isNotNull();
    }

    @Test
    @DisplayName("만료 업무 한 번이 상태 전이·이력·재고 반납을 DB 에 남긴다")
    void oneExpirationRunWritesThroughTheJpaAdapters() {
        Issuance saved = saveInTransaction(expiredIssuance());
        jdbcTemplate.update(
                "UPDATE coupon_stocks SET active_count = 1 WHERE coupon_id = ?", COUPON_ROUND_ID);

        CouponExpirationBatchResult result = runner.runOnce();

        assertThat(result.expiredCount())
                .as("만료 대상 한 건을 실제로 걷어야 한다")
                .isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM issuances WHERE id = ?", String.class, saved.id()))
                .isEqualTo("EXPIRED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM issuance_histories WHERE issuance_id = ?",
                Integer.class, saved.id()))
                .as("이력이 없으면 만료가 언제 무슨 근거로 일어났는지 남지 않는다")
                .isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT active_count FROM coupon_stocks WHERE coupon_id = ?",
                Integer.class, COUPON_ROUND_ID))
                .as("만료는 재고를 되돌려야 한다 — 조건부 갱신이 도는 자리다")
                .isZero();
    }

    /**
     * 어댑터가 {@code entityManager.refresh} 를 부르므로 쓰기는 트랜잭션 안이어야 한다.
     * 운영에서는 호출자가 열어 준다 — 여기서는 batch 컨텍스트의 트랜잭션 매니저로 직접 연다.
     * 그 매니저가 JPA 로 바뀐 것이 이 병합의 결과이고, 이 호출이 그것을 실제로 태운다.
     */
    private Issuance saveInTransaction(Issuance issuance) {
        return new TransactionTemplate(transactionManager)
                .execute(status -> issuanceRepository.save(issuance));
    }

    /** 만료 대상. 유효기간이 이미 지난 발급이다. */
    private Issuance expiredIssuance() {
        Instant issuedAt = Instant.now().truncatedTo(ChronoUnit.MICROS)
                .minus(30, ChronoUnit.DAYS);
        return Issuance.issue(COUPON_ROUND_ID, MEMBER_ID, CODE,
                MembershipGrade.GOLD, 7, issuedAt);
    }

    /**
     * 만료가 성립하려면 회차·재고·회원이 먼저 있어야 한다. 이 SQL 은 스키마 그대로라
     * 마이그레이션이 컬럼을 바꾸면 여기서 먼저 깨진다.
     */
    private void saveOpenCouponRound() {
        jdbcTemplate.batchUpdate("INSERT INTO grades (code, bit_value) VALUES (?, ?)",
                List.of(new Object[]{"WELCOME", 1}, new Object[]{"SILVER", 2},
                        new Object[]{"GOLD", 4}, new Object[]{"VIP", 8}));
        jdbcTemplate.update("INSERT INTO brands (id, name, category) VALUES (?, ?, ?)",
                1L, "테스트 브랜드", "카페");
        jdbcTemplate.update("""
                INSERT INTO coupon_templates (
                    id, brand_id, name, policy_type,
                    discount_amount, valid_days, nth_week, day_of_week,
                    start_time, duration_hours, stock_per_occurrence,
                    eligible_grades_mask, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                1L, 1L, "골드 VIP 5천원 할인", "FIXED_AMOUNT",
                5_000, 7, 3, "TUE", LocalTime.of(14, 0), 2, 10, 12, true);
        jdbcTemplate.update("""
                INSERT INTO coupons (
                    id, template_id, brand_id, name, policy_type,
                    discount_amount, valid_days, eligible_grades_mask,
                    open_at, close_at, status, generated_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                COUPON_ROUND_ID, 1L, 1L, "골드 VIP 5천원 할인", "FIXED_AMOUNT",
                5_000, 7, 12,
                LocalDateTime.of(2026, 8, 18, 5, 0), LocalDateTime.of(2026, 8, 18, 7, 0),
                "OPEN", LocalDateTime.of(2026, 8, 17, 0, 0),
                LocalDateTime.of(2026, 8, 17, 0, 1));
        jdbcTemplate.update("""
                INSERT INTO coupon_stocks (coupon_id, total_quantity, active_count, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                COUPON_ROUND_ID, 10, 0, LocalDateTime.of(2026, 8, 17, 0, 0));
        jdbcTemplate.update("""
                INSERT INTO members (id, membership_grade, created_at) VALUES (?, ?, ?)
                """,
                MEMBER_ID, "GOLD", LocalDateTime.of(2026, 1, 1, 0, 0));
    }
}
