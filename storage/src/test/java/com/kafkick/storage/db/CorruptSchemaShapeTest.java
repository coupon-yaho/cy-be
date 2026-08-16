// CORRUPT 스키마에서 CLEAN 전용 제약 셋이 실제로 없는지 확인합니다.
package com.kafkick.storage.db;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * <b>오염이 물리적으로 가능한지를 먼저 고정합니다.</b> 제약이 하나라도 남아 있으면 주입이
 * INSERT 단계에서 튕겨 <b>검출할 대상 자체가 안 생기고</b>, 규칙 테스트는 "0건" 을 정상으로
 * 읽습니다 — 규칙이 틀려도 초록입니다. 그래서 규칙보다 이 검사가 먼저 있어야 합니다.
 *
 * <p>이름을 직접 조회합니다. {@code INSERT} 가 되는지로 확인하면 <b>다른 이유로 성공해도</b>
 * 통과합니다 — 예를 들어 시드가 회원을 매번 새로 만들면 {@code uk_coupon_member} 가 살아 있어도
 * 중복이 안 생겨서 초록이 됩니다.
 */
@CorruptRepositoryTest
class CorruptSchemaShapeTest {

    @Autowired
    private JdbcClient jdbcClient;

    private List<String> indexNamesOn(String table) {
        return jdbcClient.sql("""
                        SELECT DISTINCT index_name
                          FROM information_schema.statistics
                         WHERE table_schema = DATABASE() AND table_name = :table
                        """)
                .param("table", table)
                .query(String.class)
                .list();
    }

    /**
     * <b>이름이 아니라 성질을 본다.</b> 같은 컬럼에 다른 이름의 UNIQUE 가 살아 있으면
     * 이름 검사는 통과하는데 오염은 여전히 못 심는다. 실제로 시드 저장소는 이 인덱스를
     * {@code uk_coupon_code} 로 부르고 cy-be 는 인라인 UNIQUE 라 이름이 {@code code} 다 —
     * 이름으로 고정하면 <b>둘 중 하나를 통일하는 순간 검사가 공허해진다.</b>
     */
    private List<String> uniqueLeadColumnsOn(String table) {
        return jdbcClient.sql("""
                        SELECT DISTINCT column_name
                          FROM information_schema.statistics
                         WHERE table_schema = DATABASE() AND table_name = :table
                           AND non_unique = 0 AND seq_in_index = 1
                           AND index_name <> 'PRIMARY'
                        """)
                .param("table", table)
                .query(String.class)
                .list();
    }

    @Test
    @DisplayName("coupon_id 를 선두로 한 UNIQUE 가 없다 — 오염 유형 6 이 INSERT 될 수 있어야 한다")
    void dropUniqueCouponMember() {
        assertThat(uniqueLeadColumnsOn("issuances"))
                .as("이름이 무엇이든 (coupon_id, ...) UNIQUE 가 남으면 같은 회원 2건을 못 심는다")
                .doesNotContain("coupon_id");
    }

    @Test
    @DisplayName("code 에 UNIQUE 가 없다 — 오염 유형 5 가 같은 code 를 복제한다")
    void dropUniqueIssuanceCode() {
        assertThat(uniqueLeadColumnsOn("issuances")).doesNotContain("code");
    }

    @Test
    @DisplayName("ck_stock_range 가 없다 — 오염 유형 1·3 이 재고를 범위 밖으로 민다")
    void dropStockRangeCheck() {
        List<String> checks = jdbcClient.sql("""
                        SELECT constraint_name
                          FROM information_schema.table_constraints
                         WHERE table_schema = DATABASE()
                           AND table_name = 'coupon_stocks'
                           AND constraint_type = 'CHECK'
                        """)
                .query(String.class)
                .list();

        assertThat(checks).doesNotContain("ck_stock_range");
    }

    /**
     * <b>더한 것도 고정한다.</b> 보조 인덱스가 일부러 없는 것이 이 과제의 측정 전제인데,
     * 여기에 컬럼이 하나 더 붙으면 <b>테스트만 인덱스를 갖고 실제 오염셋은 안 갖는 상태</b>가 되어
     * 성능 실험의 결론이 옮겨지지 않는다. 시드 CORRUPT 는 FK 자동 인덱스라 단일 컬럼이다.
     */
    @Test
    @DisplayName("대체 인덱스는 coupon_id 단일이다 — 시드 CORRUPT 의 FK 자동 인덱스와 같은 모양이어야 한다")
    void keepSubstituteIndexSingleColumn() {
        List<String> columns = jdbcClient.sql("""
                        SELECT column_name
                          FROM information_schema.statistics
                         WHERE table_schema = DATABASE() AND table_name = 'issuances'
                           AND index_name = 'idx_issuance_coupon'
                         ORDER BY seq_in_index
                        """)
                .query(String.class)
                .list();

        assertThat(columns).containsExactly("coupon_id");
    }

    /**
     * 떨어뜨리지 <b>않아야</b> 하는 것까지 같이 본다. {@code V900} 이 과하게 지우면
     * 오염과 무관한 사고(회차 중복 등)가 CORRUPT 에서만 조용히 통과하게 된다.
     */
    @Test
    @DisplayName("공통 제약은 남아 있다 — CORRUPT 라고 전부 푸는 것이 아니다")
    void keepCommonConstraints() {
        assertThat(indexNamesOn("coupons")).contains("uk_template_open");
        assertThat(indexNamesOn("verification_findings")).contains("uk_run_finding");
        assertThat(indexNamesOn("expected_findings")).contains("uk_expected", "idx_expected_type");
    }
}
