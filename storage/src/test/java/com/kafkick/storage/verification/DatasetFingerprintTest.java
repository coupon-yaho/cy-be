// 데이터셋 지문이 계약 공식과 같고 재료 다섯에 모두 반응하는지 확인합니다.
package com.kafkick.storage.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.HexFormat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>{@code findings_checksum} 과 짝으로만 뜻이 있는 값입니다.</b> 판정표가 읽는 것은 그 조합이고,
 * <i>"지문은 같은데 checksum 이 다르다 → 검증기 버그"</i> 가 이 프로젝트가 진짜로 잡고 싶은 칸입니다.
 * 지문이 데이터 변경에 <b>둔감하면</b> 그 칸에 데이터 사고가 잘못 떨어집니다.
 *
 * <p>그래서 두 가지를 따로 봅니다 — 계약 공식과 <b>글자 단위로 같은가</b>, 그리고
 * <b>재료 다섯이 각각 지문을 움직이는가</b>. 하나라도 안 움직이면 그 축의 변경이 안 보입니다.
 */
@RepositoryTest
@Import(VerificationRuleJdbcAdapter.class)
class DatasetFingerprintTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    @Autowired
    private VerificationRuleJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed data;

    @BeforeEach
    void setUp() {
        data = new VerificationSeed(jdbcClient);
    }

    /** 계약({@code fingerprint.formula})을 손으로 만든다. 구현을 부르면 대조가 아니라 반복이다. */
    private String contractFingerprint() {
        String material = jdbcClient.sql("""
                        SELECT CONCAT_WS('|',
                                 COALESCE((SELECT MAX(id) FROM issuance_histories
                                            WHERE created_at <= :asOf), '0'),
                                 (SELECT COUNT(*) FROM issuance_histories WHERE created_at <= :asOf),
                                 (SELECT COUNT(*) FROM issuances),
                                 COALESCE((SELECT SUM(active_count) FROM coupon_stocks), '0'),
                                 COALESCE(DATE_FORMAT((SELECT MAX(updated_at) FROM issuances),
                                          '%Y-%m-%d %H:%i:%s.%f'),
                                          '1970-01-01 00:00:00.000000'))
                        """)
                .param("asOf", AS_OF)
                .query(String.class)
                .single();

        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(material.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private long issuanceWithHistory() {
        long issuanceId = data.issuance(IssuanceStatus.ISSUED);
        data.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(1));
        return issuanceId;
    }

    @Test
    @DisplayName("계약 공식과 글자 단위로 같다 — 자기 자신과 같은 것만으로는 못 잡는다")
    void matchContractFormula() {
        issuanceWithHistory();

        assertThat(adapter.datasetFingerprint(AS_OF)).isEqualTo(contractFingerprint());
    }

    @Test
    @DisplayName("데이터가 그대로면 지문이 같다 — 정상 재실행이 거부되면 안 된다")
    void keepStableWhenNothingChanges() {
        issuanceWithHistory();

        assertThat(adapter.datasetFingerprint(AS_OF)).isEqualTo(adapter.datasetFingerprint(AS_OF));
    }

    // ─────────────────── 재료 다섯이 각각 지문을 움직이는가 ───────────────────

    @Test
    @DisplayName("이력이 늘면 지문이 달라진다 — max(id) 와 count 두 축이다")
    void reactToHistory() {
        long issuanceId = issuanceWithHistory();
        String before = adapter.datasetFingerprint(AS_OF);

        data.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, AS_OF.minusMinutes(30));

        assertThat(adapter.datasetFingerprint(AS_OF)).isNotEqualTo(before);
    }

    @Test
    @DisplayName("발급건이 늘면 지문이 달라진다")
    void reactToIssuanceCount() {
        issuanceWithHistory();
        String before = adapter.datasetFingerprint(AS_OF);

        data.issuance(IssuanceStatus.ISSUED);

        assertThat(adapter.datasetFingerprint(AS_OF)).isNotEqualTo(before);
    }

    @Test
    @DisplayName("재고 합이 바뀌면 지문이 달라진다")
    void reactToStockTotal() {
        issuanceWithHistory();
        String before = adapter.datasetFingerprint(AS_OF);

        data.overwriteStock(5);

        assertThat(adapter.datasetFingerprint(AS_OF)).isNotEqualTo(before);
    }

    /**
     * <b>소수 초까지 본다.</b> 계약이 시각 포맷을 {@code %f}(마이크로초 6자리)로 정했다.
     * 초 단위로 자르면 같은 초 안의 갱신이 지문에 안 나타나 <b>데이터가 바뀌었는데 같은 지문</b>이 된다.
     */
    @Test
    @DisplayName("발급건 갱신 시각이 소수 초만 달라도 지문이 달라진다")
    void reactToSubSecondUpdatedAt() {
        long issuanceId = issuanceWithHistory();
        jdbcClient.sql("UPDATE issuances SET updated_at = :at WHERE id = :id")
                .param("at", AS_OF.minusHours(2).withNano(100_000_000))
                .param("id", issuanceId)
                .update();
        String before = adapter.datasetFingerprint(AS_OF);

        jdbcClient.sql("UPDATE issuances SET updated_at = :at WHERE id = :id")
                .param("at", AS_OF.minusHours(2).withNano(200_000_000))
                .param("id", issuanceId)
                .update();

        assertThat(adapter.datasetFingerprint(AS_OF)).isNotEqualTo(before);
    }

    // ─────────────────────────── 경계 ───────────────────────────

    /**
     * 계약이 이력에만 {@code created_at <= asOf} 를 걸었다. 그 필터가 빠지면
     * <b>asOf 이후 이력이 지문에 들어가</b> 같은 asOf 재실행이 갈린다.
     */
    @Test
    @DisplayName("asOf 이후 이력은 지문에 안 들어간다")
    void cutHistoryAtAsOf() {
        long issuanceId = issuanceWithHistory();
        String before = adapter.datasetFingerprint(AS_OF);

        data.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, AS_OF.plusSeconds(1));

        assertThat(adapter.datasetFingerprint(AS_OF)).isEqualTo(before);
    }

    /**
     * <b>빈 데이터셋의 폴백은 시드 참조 구현과 같아야 한다.</b>
     * {@code cy-seed/seedgen/stats.py} 가 숫자 {@code 0} · 시각 {@code 1970-01-01 00:00:00.000000}
     * 을 쓴다. 시드 매니페스트와 배치 지문은 <b>대조하라고 있는 값</b>이라, 폴백이 갈리면
     * 같은 데이터에 다른 지문이 나온다.
     */
    @Test
    @DisplayName("빈 데이터셋의 폴백이 시드 참조 구현과 같다")
    void distinguishEmptyDataset() {
        String empty = adapter.datasetFingerprint(AS_OF);

        assertThat(empty).isNotNull().isEqualTo(contractFingerprint());

        issuanceWithHistory();

        assertThat(adapter.datasetFingerprint(AS_OF)).isNotEqualTo(empty);
    }
}
