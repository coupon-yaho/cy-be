package com.kafkick.storage.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.TimeZone;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.verification.replay.IssuanceHistoryRecord;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>JVM 기본 타임존을 바꿔 놓고 본다.</b> 다른 테스트가 이걸 못 잡는 이유는 시드와 어댑터가
 * 같은 변환을 쓰기 때문이다 — 서로 밀려 있어도 왕복은 맞는다.
 *
 * <p>그래서 여기서는 <b>SQL 리터럴로 직접 넣는다.</b> 시드가 만드는 값이 아니라
 * DB 에 실제로 저장된 벽시계를 기준으로 어댑터가 무엇을 돌려주는지 본다.
 *
 * <p>운영 컨테이너는 UTC 겠지만 개발 기기는 KST 다. 여기가 어긋나면
 * "로컬에서만 이상하다"가 되고, asOf 가 이 시스템의 결정론 축이라 그 순간 전부 무너진다.
 */
@RepositoryTest
@Import(ReplayHistoryJdbcAdapter.class)
class TimestampMappingTest {

    private static final TimeZone SEOUL = TimeZone.getTimeZone("Asia/Seoul");
    private static final LocalDateTime STORED = LocalDateTime.of(2026, 8, 15, 13, 0);
    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);

    @Autowired
    private ReplayHistoryJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    private TimeZone original;
    private long issuanceId;

    @BeforeEach
    void useSeoulTimeZone() {
        original = TimeZone.getDefault();
        TimeZone.setDefault(SEOUL);

        issuanceId = new VerificationSeed(jdbcClient).issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("""
                        INSERT INTO issuance_histories
                            (issuance_id, event_type, from_status, to_status, created_at)
                        VALUES (:id, 'ISSUE', NULL, 'ISSUED', '2026-08-15 13:00:00.000000')
                        """)
                .param("id", issuanceId)
                .update();
    }

    @AfterEach
    void restoreTimeZone() {
        TimeZone.setDefault(original);
    }

    @Test
    @DisplayName("저장된 벽시계를 그대로 읽는다 — JVM 타임존이 값을 밀면 안 된다")
    void readStoredWallClockUnshifted() {
        assertThat(adapter.findRange(issuanceId, issuanceId, AS_OF, Long.MAX_VALUE))
                .singleElement()
                .extracting(IssuanceHistoryRecord::createdAt)
                .isEqualTo(STORED);
    }

    @Test
    @DisplayName("asOf 가 저장된 벽시계와 같은 규약으로 비교된다 — 밀리면 이력이 통째로 잘린다")
    void compareAsOfAgainstSameWallClock() {
        assertThat(adapter.findRange(issuanceId, issuanceId, AS_OF, Long.MAX_VALUE)).hasSize(1);
    }

    @Test
    @DisplayName("asOf 를 저장 시각 직전으로 두면 잘린다 — 경계가 벽시계 기준으로 선다")
    void cutJustBeforeStoredWallClock() {
        assertThat(adapter.findRange(issuanceId, issuanceId, STORED.minusNanos(1_000), Long.MAX_VALUE)).isEmpty();
    }
}
