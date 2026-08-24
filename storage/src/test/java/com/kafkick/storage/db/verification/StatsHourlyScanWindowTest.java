// 요일·시각 집계를 이력 id 창으로 쪼개도 같은 값이 나오는지 확인합니다.
package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.verification.HourlyIssued;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>{@code StatsJdbcAdapterTest} 는 이 축을 못 잰다.</b> 그쪽은 기본 창(50만)으로 돌아
 * 어떤 픽스처를 넣어도 <b>창이 언제나 하나</b>다 — 쪼개기가 실제로 일어나는 경로를 한 줄도
 * 지나지 않는다. 그래서 창을 하한({@link StatsJdbcAdapter#MIN_HISTORY_SCAN_WINDOW})까지
 * 좁히고, 그것만으로도 모자라 <b>이력 id 를 그 폭보다 넓게 벌려</b> 심는다
 * ({@link #issueAt} 이 id 를 직접 준다). 안 벌리면 픽스처 전부가 첫 창에 들어가
 * <b>단언이 전부 통과하면서 아무것도 안 지킨다.</b>
 *
 * <p>⚠️ <b>{@code AUTO_INCREMENT} 를 밀면 안 된다.</b> {@code ALTER TABLE} 은 MySQL 에서
 * <b>암묵 커밋</b>이라 테스트 트랜잭션이 그 자리에서 끝나고, 심은 행이 롤백되지 않아
 * 다음 테스트가 {@code uk_coupon_code} 중복으로 죽는다(실제로 밟았다). id 를 직접 주는 것은
 * DML 이라 그 문제가 없다.
 *
 * <p>⚠️ <b>창을 1로는 못 준다.</b> 생성자가 거절한다 — 516만 셋에서 왕복이 500만 회를 넘어
 * Step 이 데드라인에 걸리는데, 그때 나는 알림({@code VerifyNotSucceeding})의 runbook 이
 * 원인과 다른 곳을 가리킨다. 그 거절 자체는 {@link #rejectWindowOutsideBounds} 가 잰다.
 *
 * <p>쪼개기가 들어오면서 <b>새로 생긴 실패 방식이 셋</b>이고 이 클래스가 그 셋을 잰다:
 * <pre>
 * 중복 키   같은 (요일, 시각) 이 두 창에 걸치면 부분합이 둘 나온다. 접지 않고 돌려주면
 *          호출부의 HourlyIssued.fillAll 이 Collectors.toMap 에서 IllegalStateException 이다
 * 경계     (from, to] 규약이 어긋나면 경계 id 를 두 창이 함께 세거나 아무도 안 센다
 * 폭주     창이 너무 좁으면 왕복이 폭증해 Step 이 데드라인에 걸린다
 * </pre>
 *
 * @see StatsJdbcAdapter#MAX_HISTORY_SCAN_WINDOW 창을 쪼개는 근거(CY-470 실측)
 */
@RepositoryTest
@Import({StatsJdbcAdapter.class, VerificationRunJdbcAdapter.class})
@TestPropertySource(properties = "batch.verify.history-scan-window=10000")
class StatsHourlyScanWindowTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /** {@code StatsJdbcAdapterTest} 와 같은 기준 시각. 2025-01-01 은 <b>수요일</b>이다. */
    private static final LocalDateTime OPEN_AT = LocalDateTime.of(2025, 1, 1, 0, 0);

    /** 얼린 이력 상한을 넘는 값. 상한을 안 좁히는 테스트에서 쓴다. */
    private static final long NO_LIMIT = Long.MAX_VALUE;

    /**
     * 위 {@code @TestPropertySource} 와 <b>같은 값이어야 한다</b> — 픽스처가 이 폭으로 id 를
     * 벌려 이력 하나가 창 하나를 차지하게 만든다.
     */
    private static final long WINDOW = StatsJdbcAdapter.MIN_HISTORY_SCAN_WINDOW;

    @Autowired
    private StatsJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed seed;

    /** 다음에 심을 이력의 id. 창 폭씩 벌려 이력 하나가 창 하나를 차지하게 한다. */
    private long nextHistoryId = 1L;

    @BeforeEach
    void setUp() {
        seed = new VerificationSeed(jdbcClient);
        nextHistoryId = 1L;
    }

    /**
     * 이력 하나를 <b>지정한 id 로</b> 심는다. 그 id 가 창 폭씩 벌어져 있어야 쪼개기 경로를
     * 실제로 지난다.
     *
     * <p>id 에 구멍이 나는 것은 실제 형상과도 가깝다 — 운영의 이력 id 는 롤백과 삭제로
     * 띄엄띄엄해진다.
     */
    private long issueAt(LocalDateTime at) {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        long historyId = nextHistoryId;
        nextHistoryId += WINDOW + 1;

        jdbcClient.sql("""
                        INSERT INTO issuance_histories
                            (id, issuance_id, event_type, from_status, to_status, created_at)
                        VALUES (:id, :issuanceId, 'ISSUE', NULL, 'ISSUED', :createdAt)
                        """)
                .param("id", historyId)
                .param("issuanceId", issuanceId)
                .param("createdAt", at)
                .update();
        return historyId;
    }

    /**
     * <b>접지 않으면 호출부가 죽는다.</b> {@code HourlyIssued.fillAll} 이
     * {@code Collectors.toMap} 으로 칸을 찾으므로 같은 키가 둘이면 그 자리에서
     * {@code IllegalStateException} 이다. 그리고 300만 시드는 두 달치라
     * <b>모든 칸이 여러 창에 걸친다</b> — 즉 안 접으면 언제나 죽는다.
     */
    @Test
    @DisplayName("같은 요일·시각이 여러 창에 걸쳐도 한 행으로 접힌다")
    void mergePartialsOfTheSameSlot() {
        issueAt(OPEN_AT.plusHours(13));
        issueAt(OPEN_AT.plusHours(13));
        issueAt(OPEN_AT.plusHours(13));

        List<HourlyIssued> measured = adapter.issuedByHour(NO_LIMIT, AS_OF);

        assertThat(measured)
                .as("id 를 창 폭보다 넓게 벌려 심었으므로 셋이 서로 다른 질의에서 왔다")
                .containsExactly(new HourlyIssued("WED", 13, 3));
        assertThat(HourlyIssued.fillAll(measured))
                .as("접힌 뒤라야 168칸 채우기가 산다")
                .hasSize(7 * 24);
    }

    /**
     * <b>{@code (from, to]} 규약이 경계를 정확히 나눈다.</b> 한쪽이라도 닫히거나 열리는 방향이
     * 틀리면 경계 id 를 두 창이 함께 세거나(과대) 아무도 안 센다(과소).
     */
    @Test
    @DisplayName("창 경계의 이력을 겹쳐 세지도 빠뜨리지도 않는다")
    void partitionIdRangeExactly() {
        for (int hour = 0; hour < 5; hour++) {
            issueAt(OPEN_AT.plusHours(hour));
        }

        List<HourlyIssued> measured = adapter.issuedByHour(NO_LIMIT, AS_OF);

        assertThat(measured.stream().mapToInt(HourlyIssued::issuedTotal).sum())
                .as("이력 다섯을 다섯 창에 나눠 담아도 합은 다섯이다")
                .isEqualTo(5);
        assertThat(measured)
                .containsExactlyInAnyOrder(
                        new HourlyIssued("WED", 0, 1),
                        new HourlyIssued("WED", 1, 1),
                        new HourlyIssued("WED", 2, 1),
                        new HourlyIssued("WED", 3, 1),
                        new HourlyIssued("WED", 4, 1));
    }

    /**
     * 얼린 상한은 창을 쪼개도 그대로다. 창 경계를 도는 쪽이 상한을 잊으면
     * <b>리플레이가 못 읽은 이력이 통계에만 들어와</b> 같은 {@code asOf} 재실행이
     * 다른 값을 낸다.
     *
     * <p><b>불변식이 두 자리에 산다.</b> 루프의 끝({@code scanCeiling})과 창 질의 자신의
     * {@code id <= :maxHistoryId} 다. 둘 중 하나만 남겨도 지금은 통과하지만, 남긴 쪽을
     * 나중에 고치는 사람이 다른 쪽이 있다는 것을 모르면 조용히 샌다.
     */
    @Test
    @DisplayName("창을 쪼개도 얼린 상한 밖 이력은 안 센다")
    void respectFrozenBoundaryAcrossWindows() {
        issueAt(OPEN_AT.plusHours(3));
        long frozen = issueAt(OPEN_AT.plusHours(3));
        issueAt(OPEN_AT.plusHours(3));

        assertThat(adapter.issuedByHour(frozen, AS_OF))
                .as("셋째 이력은 asOf 이하지만 얼린 상한 밖이다")
                .containsExactly(new HourlyIssued("WED", 3, 2));
    }

    /** 셀 행이 없으면 창을 한 번도 안 돈다 — {@code Long.MAX_VALUE} 상한이어도 그렇다. */
    @Test
    @DisplayName("ISSUE 이력이 없으면 빈 목록이고 루프가 안 돈다")
    void returnEmptyWithoutIssueHistory() {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        seed.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, OPEN_AT.plusHours(2));

        assertThat(adapter.issuedByHour(NO_LIMIT, AS_OF)).isEmpty();
    }

    /**
     * <b>위아래 양쪽을 막는다.</b>
     *
     * <p>0 이하는 {@code for (from …; from += 창)} 이 전진하지 않아 <b>끝나지 않는다</b> —
     * 예외 없이 매달린다. 상한 위쪽은 이 손잡이가 막으려던 사고(MySQL TempTable 이 RAM 을
     * 안 놓아 <b>DB 서버가 죽는 것</b>)를 그대로 되돌린다.
     *
     * <p><b>하한이 1이 아닌 것이 요지다.</b> 1 은 0 이 아니라 통과하는데, 그 값이면 516만
     * 셋에서 왕복이 500만 회를 넘어 Step 이 {@code batch.verify.step-timeout-ms} 에 걸린다 —
     * 그때 나는 알림은 {@code VerifyNotSucceeding} 이고 그 runbook 은 <i>"그 슬롯에 발급이
     * 있었다"</i> 로 사람을 보낸다. 원인과 처방이 완전히 다른 곳이다.
     */
    @Test
    @DisplayName("창이 하한 아래이거나 상한을 넘으면 기동 때 거절한다")
    void rejectWindowOutsideBounds() {
        assertThatThrownBy(() -> new StatsJdbcAdapter(null, null, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.verify.history-scan-window");

        assertThatThrownBy(() -> new StatsJdbcAdapter(
                null, null, StatsJdbcAdapter.MIN_HISTORY_SCAN_WINDOW - 1))
                .as("하한 바로 아래가 통과하면 1 도 통과한다 — 그 값이 실제 사고다")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("step-timeout-ms");

        assertThatThrownBy(() -> new StatsJdbcAdapter(
                null, null, StatsJdbcAdapter.MAX_HISTORY_SCAN_WINDOW + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CY-470");
    }
}
