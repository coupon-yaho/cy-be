// CLEAN 전용 제약 셋이 실제로 위반을 거부하는지 확인합니다. 존재가 아니라 강제력을 봅니다.
package com.kafkick.storage.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * <b>{@link CorruptSchemaShapeTest} 와 대칭입니다.</b> 그쪽은 CORRUPT 에서 셋이 <b>없음</b>을 보고,
 * 여기서는 CLEAN 에서 셋이 <b>막음</b>을 봅니다.
 *
 * <p><b>존재 확인만으로는 부족합니다.</b> 제약이 있되 강제되지 않는 상태가 MySQL 에 실재합니다 —
 * {@code CHECK} 는 {@code NOT ENFORCED} 로 만들 수 있고 UNIQUE 인덱스는 비유니크로 다시
 * 만들 수 있습니다. 그러면 마이그레이션도 {@code V9999999999} 도 형태 검사도 전부 초록인데
 * <b>CLEAN 에 오염이 조용히 들어갑니다.</b>
 *
 * <p>"정상셋에서 V2 검출이 나올 수 없다" 는 이 프로젝트의 주장이 전적으로 이 셋의 강제력에
 * 얹혀 있습니다. 그 주장을 지키는 것이 이 클래스입니다.
 */
@RepositoryTest
class CleanSchemaConstraintTest {

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed data;

    @BeforeEach
    void setUp() {
        data = new VerificationSeed(jdbcClient);
    }

    @Test
    @DisplayName("uk_coupon_member 가 같은 회원의 두 번째 발급을 막는다 — 오염 유형 6")
    void rejectSecondIssuanceForSameMember() {
        long memberId = data.issuanceForNewMember();

        assertThatThrownBy(() -> data.issuanceForMember(memberId))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("code UNIQUE 가 발급코드 복제를 막는다 — 오염 유형 5")
    void rejectDuplicatedCode() {
        data.issuanceForNewMember();
        String code = data.currentIssuanceCode();

        assertThatThrownBy(() -> data.issuanceForNewMemberWithCode(code))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    /**
     * <b>{@code V2026082504__stock_range_check.sql} 의 유일한 존재 증명이다.</b> 이 단언이 없으면
     * 그 마이그레이션이 무엇을 하는지 확인하는 코드가 저장소에 하나도 없다.
     *
     * <p><b>{@code DataIntegrityViolationException} 이 아니다.</b> MySQL 의 CHECK 위반(3819)은
     * 스프링의 오류 코드 번역표에 없어 {@code UncategorizedSQLException} 으로 올라온다.
     * UNIQUE 위반과 예외 타입이 다르므로, 타입만 보고 "제약이 안 걸렸다" 고 읽으면 안 된다.
     * 제약 이름까지 확인해 <b>어느 제약이 걸렸는지</b>를 못 박는다.
     */
    @Test
    @DisplayName("ck_stock_range 가 재고 초과를 막는다 — 오염 유형 1")
    void rejectStockAboveTotal() {
        data.currentCouponIdOrCreate();

        // ⚠️ **제약이 둘이다(CY-744).** main 의 V3 가 같은 불변식을
        //    ck_coupon_stock_active_range 로 한 겹 더 걸어 뒀고, 그쪽이 먼저 걸린다.
        //    이름을 하나로 못 박으면 **막혔는데도 빨개진다** — 여기서 재려는 것은
        //    "CLEAN 이 오염 유형 1 을 막는가" 이지 어느 제약이 막는가가 아니다.
        //    둘 다 CORRUPT 에서는 떨어진다(V9999999999).
        assertThatThrownBy(() -> data.overwriteStock(101))
                .as("total_quantity 가 100 이다")
                .isInstanceOf(DataAccessException.class)
                .hasMessageMatching("(?s).*(ck_stock_range|ck_coupon_stock_active_range).*");
    }

    @Test
    @DisplayName("ck_stock_range 가 음수 재고를 막는다 — 오염 유형 3")
    void rejectNegativeStock() {
        data.currentCouponIdOrCreate();

        assertThatThrownBy(() -> data.overwriteStock(-1))
                .isInstanceOf(DataAccessException.class)
                .hasMessageMatching("(?s).*(ck_stock_range|ck_coupon_stock_active_range).*");
    }

    @Test
    @DisplayName("범위 안의 재고는 통과한다 — 제약이 정상 데이터를 막으면 안 된다")
    void acceptStockWithinRange() {
        data.currentCouponIdOrCreate();

        assertThatCode(() -> data.overwriteStock(100)).doesNotThrowAnyException();
        assertThatCode(() -> data.overwriteStock(0)).doesNotThrowAnyException();
    }

    /**
     * <b>{@code V3}·{@code V4} 가 넓힌 정밀도를 고정한다.</b> 소수 초가 잘리면 V5 의 활성 판정
     * ({@code used_at <= asOf AND (canceled_at IS NULL OR canceled_at > asOf)})이 경계에서
     * 뒤집혀 <b>오염이 없는 CLEAN 에서 V5 가 운다.</b> 정상셋 0건이 합격 조건이라 게이트가 떨어진다.
     *
     * <p>시드 저장소 DDL 도 같은 정밀도여야 한다 — 그쪽이 실제 게이트 스키마를 만들고
     * 이 마이그레이션은 거기 닿지 않는다.
     */
    @Test
    @DisplayName("시각 컬럼이 datetime(6) 이다 — 소수 초가 잘리면 CLEAN 에서 V5 가 운다")
    void keepSubSecondPrecision() {
        assertThat(precisionOf("issuance_usages", "used_at")).isEqualTo(6);
        assertThat(precisionOf("issuance_usages", "canceled_at")).isEqualTo(6);
        assertThat(precisionOf("coupon_stocks", "updated_at")).isEqualTo(6);
        assertThat(precisionOf("issuances", "updated_at")).isEqualTo(6);
    }

    private int precisionOf(String table, String column) {
        return jdbcClient.sql("""
                        SELECT datetime_precision
                          FROM information_schema.columns
                         WHERE table_schema = DATABASE()
                           AND table_name = :table AND column_name = :column
                        """)
                .param("table", table)
                .param("column", column)
                .query(Integer.class)
                .single();
    }

    /**
     * {@code clear()} 는 잡 테스트가 쓰는 비-트랜잭션 경로다. 여기서 {@code lastCode} 를
     * 안 비우면 <b>지운 발급의 code 를 복제 원본으로 집게 되어</b>, 케이스 2 를 심었다고
     * 믿는데 실제로는 정상 데이터인 테스트가 만들어진다.
     */
    @Test
    @DisplayName("clear() 뒤에는 복제할 원본 code 가 없다")
    void forgetLastCodeOnClear() {
        data.issuanceForNewMember();
        data.clear();

        assertThatThrownBy(() -> data.currentIssuanceCode())
                .isInstanceOf(IllegalStateException.class);
    }
}
