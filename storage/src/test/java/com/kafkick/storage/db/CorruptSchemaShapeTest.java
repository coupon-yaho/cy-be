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
     * <b>이름도 선두 컬럼도 아니라 "무엇을 막는가" 를 본다.</b>
     *
     * <p>이름으로 고정하면 두 저장소가 같은 인덱스를 다르게 부르는 순간 검사가 공허해진다.
     * 실제로 그런 상태였다 — 시드는 {@code uk_coupon_code}, cy-be 는 인라인 UNIQUE 라 이름이
     * {@code code} 였다({@code V2026082505__name_unique_constraints.sql} 이 맞췄다).
     * 이름을 맞춘 뒤에도 이 검사는 이름을 보지 않는다 — 다시 갈릴 수 있고, 그때 공허해지면 안 된다.
     * 선두 컬럼만 봐도 부족하다 — {@code UNIQUE(member_id, coupon_id)} 는 선두가 {@code member_id}
     * 라 {@code coupon_id} 검사를 통과하는데 <b>유형 6 을 여전히 막는다.</b>
     *
     * <p>정확한 판정은 이것이다. 오염이 심는 두 행은 <b>몇 개 컬럼만 다르다.</b>
     * 어떤 UNIQUE 든 그 다른 컬럼을 <b>하나도 안 갖고 있으면</b> 두 행이 충돌해 INSERT 가 튕긴다.
     */
    private List<List<String>> uniqueIndexColumnsOn(String table) {
        List<String> names = jdbcClient.sql("""
                        SELECT DISTINCT index_name
                          FROM information_schema.statistics
                         WHERE table_schema = DATABASE() AND table_name = :table
                           AND non_unique = 0
                        """)
                .param("table", table)
                .query(String.class)
                .list();

        return names.stream()
                .map(name -> jdbcClient.sql("""
                                SELECT column_name
                                  FROM information_schema.statistics
                                 WHERE table_schema = DATABASE() AND table_name = :table
                                   AND index_name = :name
                                 ORDER BY seq_in_index
                                """)
                        .param("table", table)
                        .param("name", name)
                        .query(String.class)
                        .list())
                .toList();
    }

    /** 두 행이 서로 다른 컬럼을 하나도 안 가진 UNIQUE 는 그 둘의 공존을 막는다. */
    private void assertNothingBlocks(String table, String what, String... differingColumns) {
        List<String> differing = List.of(differingColumns);

        assertThat(uniqueIndexColumnsOn(table))
                .as("%s 를 막는 UNIQUE 가 남아 있다 — 오염을 심을 수 없어 검출 0건이 정상으로 보인다", what)
                .allSatisfy(columns -> assertThat(columns).containsAnyElementsOf(differing));
    }

    @Test
    @DisplayName("같은 회원의 두 번째 발급을 막는 UNIQUE 가 없다 — 오염 유형 6")
    void dropUniqueCouponMember() {
        // 유형 6 의 두 행은 id 와 code 만 다르다.
        assertNothingBlocks("issuances", "같은 회원의 두 번째 발급", "id", "code");
    }

    @Test
    @DisplayName("같은 code 의 복제를 막는 UNIQUE 가 없다 — 오염 유형 5")
    void dropUniqueIssuanceCode() {
        // 유형 5 의 두 행은 id 와 member_id 만 다르다.
        assertNothingBlocks("issuances", "같은 code 의 복제", "id", "member_id");
    }

    /**
     * <b>대체 인덱스를 더 안 만든다(CY-744).</b>
     *
     * <p>예전에는 {@code uk_coupon_member} 를 떼면 {@code coupon_id} FK 가 쓸 인덱스가
     * 없어져서, {@code V9999999999} 가 {@code coupon_id} 단일 인덱스를 하나 만들었다.
     *
     * <p><b>이제 만들 필요가 없다.</b> main 의
     * {@code V2026082502__add_admin_issuance_history_indexes.sql} 이
     * {@code idx_issuances_coupon_id (coupon_id, id)} 를 CLEAN·CORRUPT 양쪽에 만들고,
     * FK 는 그것을 쓴다. 그래서 손으로 하나 더 만들면 <b>시드에 없는 인덱스가 생겨</b>
     * 스키마 파리티가 그 자리를 잡는다.
     *
     * <p>검사는 지우지 않고 <b>뒤집는다</b> — 누가 그 CREATE INDEX 를 되살리면 여기서
     * 빨개진다. 보조 인덱스가 일부러 없는 것이 이 과제의 측정 전제이기 때문이다.
     */
    @Test
    @DisplayName("손으로 만든 coupon_id 대체 인덱스가 없다 — FK 는 idx_issuances_coupon_id 를 쓴다")
    void doesNotCreateSubstituteIndex() {
        assertThat(indexColumns("coupon_id"))
                .as("main 이 idx_issuances_coupon_id 를 만든 뒤로 이것은 중복이고, "
                        + "만들면 시드 CORRUPT 에 없는 인덱스가 되어 파리티가 깨진다")
                .isEmpty();
        assertThat(indexColumns("idx_issuances_coupon_id"))
                .as("FK 를 받치는 인덱스가 사라지면 uk_coupon_member 를 뗄 수 없다")
                .containsExactly("coupon_id", "id");
    }

    private List<String> indexColumns(String indexName) {
        return jdbcClient.sql("""
                        SELECT column_name
                          FROM information_schema.statistics
                         WHERE table_schema = DATABASE() AND table_name = 'issuances'
                           AND index_name = :indexName
                         ORDER BY seq_in_index
                        """)
                .param("indexName", indexName)
                .query(String.class)
                .list();
    }

    /**
     * 떨어뜨리지 <b>않아야</b> 하는 것까지 같이 본다. {@code V9999999999} 이 과하게 지우면
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
