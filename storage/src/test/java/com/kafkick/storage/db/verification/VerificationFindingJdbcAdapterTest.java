package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ResidualCount;
import com.kafkick.core.verification.exception.VerificationErrorCode;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
@Import({VerificationFindingJdbcAdapter.class, VerificationRunJdbcAdapter.class})
class VerificationFindingJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);

    @Autowired
    private VerificationFindingJdbcAdapter adapter;

    @Autowired
    private VerificationRunJdbcAdapter runAdapter;

    @Autowired
    private JdbcClient jdbcClient;

    private long runId;

    @BeforeEach
    void setUp() {
        runId = newRun(1);
    }

    private long newRun(int attempt) {
        return runAdapter.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, attempt, AS_OF)).id();
    }

    /**
     * <b>세 갈래가 실제로 갈리는가.</b> 개수만 맞대는 {@code /reports/diff} 로는
     * <i>"같은 3건"</i> 과 <i>"다 고쳐지고 새로 3건"</i> 이 같은 모양이라, 이 조회가
     * 갈라 주지 못하면 티켓이 아무것도 안 한 것이다.
     *
     * <p>한 번에 셋을 다 심는다 — <b>세 갈래가 서로를 안 먹는지</b>까지 봐야 한다.
     * 하나씩 재면 "전부 지속" 으로 답하는 구현도 통과한다.
     */
    @Test
    @DisplayName("두 실행을 맞대면 지속·신규·해소가 갈린다")
    void residualSplitsPersistedIntroducedAndResolved() {
        // attempt 1 은 setUp 이 이미 썼다 — uk_run_params 가 (asOf, dataset, scope, attempt) 다.
        long before = newRun(2);
        long after = newRun(3);

        // ⚠️ **세 수를 다르게 만든다.** 1·1·1 로 두면 신규와 해소를 맞바꾸는 돌연변이가
        //    살아남는다 — 실제로 그렇게 썼다가 M-1 이 통과했다.
        //    지속 1(HISTORY:1) / 해소 2(HISTORY:2·4) / 신규 3(HISTORY:3·5·6)
        adapter.appendAll(before, List.of(
                finding(1), finding(2), finding(4)));
        adapter.appendAll(after, List.of(
                finding(1), finding(3), finding(5), finding(6)));

        Map<FindingType, ResidualCount> residual = adapter.residualByType(before, after);

        assertThat(residual.get(FindingType.ILLEGAL_TRANSITION))
                .as("셋이 서로 다른 수여야 방향을 바꾸는 실수가 잡힌다")
                .isEqualTo(new ResidualCount(1, 3, 2));
        assertThat(residual.get(FindingType.ILLEGAL_TRANSITION).remaining())
                .as("PRD 가 말하는 잔여 불일치 건수는 지속 + 신규다")
                .isEqualTo(4);
    }

    /**
     * <b>방향이 바뀌면 신규와 해소가 뒤집힌다.</b> {@code has_after} 를 반대로 읽거나
     * 인자 둘을 바꿔 넘기는 실수를 여기서 잡는다 — 개수만 보는 단언은 그 실수를 통과시킨다.
     */
    @Test
    @DisplayName("앞뒤를 바꿔 부르면 신규와 해소가 맞바뀐다")
    void swappingTheRunsSwapsIntroducedAndResolved() {
        long before = newRun(4);
        long after = newRun(5);
        // **비대칭이어야 한다.** 양쪽 수가 같으면 맞바꿔도 같은 답이라 아무것도 안 잰다.
        adapter.appendAll(before, List.of(finding(2)));
        adapter.appendAll(after, List.of(finding(3), finding(7)));

        assertThat(adapter.residualByType(before, after).get(FindingType.ILLEGAL_TRANSITION))
                .as("앞에만 1건, 뒤에만 2건 — 신규 2 · 해소 1")
                .isEqualTo(new ResidualCount(0, 2, 1));
        assertThat(adapter.residualByType(after, before).get(FindingType.ILLEGAL_TRANSITION))
                .as("방향을 뒤집으면 그 둘이 맞바뀐다")
                .isEqualTo(new ResidualCount(0, 1, 2));
    }

    /**
     * <b>남의 실행이 섞이면 안 된다.</b> {@code WHERE run_id IN (...)} 를 통째로 지워도
     * 다른 시험은 전부 초록이었다 — 그 시험들에는 <b>검출을 가진 제3의 실행이 없기</b>
     * 때문이다(어댑터 시험은 롤백, API 시험은 매번 검출을 지운다).
     *
     * <p>운영 DB 는 실행이 계속 쌓이는 표라 그 가드가 빠지면 <b>남의 검출이 통째로
     * 섞여</b> 지속·신규가 다 틀린다. 그것이 안 보이는 상태가 이 시험이 없던 상태다.
     */
    @Test
    @DisplayName("맞대는 두 실행 밖의 검출은 안 섞인다")
    void residualIgnoresOtherRuns() {
        long before = newRun(6);
        long after = newRun(7);
        long stranger = newRun(8);

        adapter.appendAll(before, List.of(finding(10)));
        adapter.appendAll(after, List.of(finding(10)));
        // 남의 실행이 같은 키를 갖고 있다 — 섞이면 sides 가 3 이 되어 지속이 0 으로 뒤집힌다.
        adapter.appendAll(stranger, List.of(finding(10), finding(11)));

        assertThat(adapter.residualByType(before, after).get(FindingType.ILLEGAL_TRANSITION))
                .as("HISTORY:10 하나가 지속이고, 남의 HISTORY:11 은 답에 없어야 한다")
                .isEqualTo(new ResidualCount(1, 0, 0));
    }

    /**
     * <b>규칙이 다르면 같은 대상이라도 다른 검출이다.</b> {@code REPLAY_MISMATCH} ·
     * {@code USAGE_MISMATCH} · {@code GRADE_VIOLATION} 은 전부 {@code Grain.ISSUANCE} 라
     * <b>같은 {@code ISSUANCE:<id>} 키</b>를 만든다 — 한 발급건이 두 규칙에 잡히는 것이
     * 실재한다.
     *
     * <p>안쪽 {@code GROUP BY} 에서 {@code finding_type} 을 빼면 그 둘이 한 키로 묶여
     * {@code sides = 2} 가 되고, <b>한 번도 지속된 적 없는 것이 "지속" 으로</b> 보고된다.
     * 규칙 하나만 심는 시험으로는 그 돌연변이가 안 죽는다.
     */
    @Test
    @DisplayName("같은 대상이라도 규칙이 다르면 따로 센다")
    void residualKeepsRulesApartEvenOnTheSameTarget() {
        long before = newRun(9);
        long after = newRun(10);

        // 한 발급건이 앞 실행에서는 V3, 뒤 실행에서는 V5 에 잡혔다 — 지속이 아니다.
        adapter.appendAll(before, List.of(VerificationFinding.forIssuance(
                FindingType.REPLAY_MISMATCH, 77, "기대", "실제")));
        adapter.appendAll(after, List.of(VerificationFinding.forIssuance(
                FindingType.USAGE_MISMATCH, 77, "기대", "실제")));

        Map<FindingType, ResidualCount> residual = adapter.residualByType(before, after);

        assertThat(residual.get(FindingType.REPLAY_MISMATCH))
                .as("앞에만 있었다 — 해소다")
                .isEqualTo(new ResidualCount(0, 0, 1));
        assertThat(residual.get(FindingType.USAGE_MISMATCH))
                .as("뒤에만 있다 — 신규다. 규칙을 안 가르면 이 둘이 '지속' 하나가 된다")
                .isEqualTo(new ResidualCount(0, 1, 0));
    }

    /**
     * <b>바이트가 다르면 다른 검출이다.</b> 두 컬럼에 {@code COLLATE} 가 없어 서버 기본
     * ({@code utf8mb4_0900_ai_ci})을 물려받는데 그것은 <b>대소문자를 무시</b>한다 —
     * 기본 콜레이션으로 묶으면 {@code HISTORY:20} 과 {@code history:20} 이 한 키가 되어
     * <b>한 번도 지속된 적 없는 것이 "지속" 으로</b> 잡힌다.
     *
     * <p>같은 파일의 checksum 질의가 <b>같은 이유로 같은 캐스팅</b>을 쓴다. 그 계약을
     * 이 조회도 지키는지 여기서 잰다.
     *
     * <p><b>도메인으로는 이 값을 못 만든다</b> — {@code TargetKey} 가 대문자 접두사를
     * 붙인다. 그래서 SQL 로 직접 심는다. 도달하려면 쓰기 경로가 깨져야 하지만, 그때
     * <b>조용히 틀린 답</b>이 나가는 것과 말해 주는 것은 다르다.
     */
    @Test
    @DisplayName("대소문자만 다른 키는 같은 검출이 아니다")
    void residualComparesKeysByBytes() {
        long before = newRun(11);
        long after = newRun(12);
        adapter.appendAll(before, List.of(finding(20)));
        // 같은 대상을 소문자 키로 뒤 실행에 심는다 — 기본 콜레이션이면 한 키로 합쳐진다.
        jdbcClient.sql("""
                        INSERT INTO verification_findings
                               (run_id, finding_type, target_key, history_id, expected, actual)
                        VALUES (:runId, 'ILLEGAL_TRANSITION', 'history:20', 20, '기대', '실제')
                        """)
                .param("runId", after)
                .update();

        assertThat(adapter.residualByType(before, after).get(FindingType.ILLEGAL_TRANSITION))
                .as("합쳐지면 (1,0,0) 이 된다 — 한 번도 지속된 적 없는데")
                .isEqualTo(new ResidualCount(0, 1, 1));
    }

    /** 같은 규칙·다른 대상의 검출 하나. 세 갈래를 수로 가르려면 키만 달라지면 된다. */
    private static VerificationFinding finding(long historyId) {
        return VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, historyId, "기대", "실제");
    }

    /**
     * <b>같은 실행을 두 번 주면 답이 틀린 채로 나간다.</b> {@code IN (:a, :b)} 가 두 값이
     * 같으면 한 실행만 훑고, 모든 키가 {@code sides = 1}·{@code has_after = 1} 이라
     * <b>전부 "새로 생겼다"</b> 로 나온다 — 조용한 오답이라 포트 안에서 끊는다.
     */
    @Test
    @DisplayName("같은 실행끼리는 맞대기를 거절한다")
    void refusesToCompareARunWithItself() {
        assertThatThrownBy(() -> adapter.residualByType(runId, runId))
                .as("raw 예외면 컨트롤러 가드가 빠지는 날 500 + 스프링 기본 본문으로 나간다")
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining(String.valueOf(runId));
        assertThat(catchThrowableOfType(BusinessException.class,
                () -> adapter.residualByType(runId, runId)).getErrorCode())
                .as("컨트롤러의 requireComparable 과 같은 코드여야 답이 안 갈린다")
                .isEqualTo(VerificationErrorCode.RUNS_NOT_COMPARABLE);
    }

    @Test
    @DisplayName("검출 결과를 쌓으면 그대로 들어간다")
    void appendFinding() {
        adapter.appendAll(runId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131,
                "USED-EXPIRE->(없음)", "USED-EXPIRE->EXPIRED")));

        Map<String, Object> row = findByTargetKey("HISTORY:88131");
        assertThat(row.get("finding_type")).isEqualTo("ILLEGAL_TRANSITION");
        assertThat(row.get("expected")).isEqualTo("USED-EXPIRE->(없음)");
        assertThat(row.get("actual")).isEqualTo("USED-EXPIRE->EXPIRED");
    }

    @Test
    @DisplayName("이력 단위 검출은 history_id 만 채운다 — 나머지 다형 컬럼은 NULL 이다")
    void fillOnlyHistoryColumn() {
        adapter.appendAll(runId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131, "a", "b")));

        Map<String, Object> row = findByTargetKey("HISTORY:88131");
        assertThat(row.get("history_id")).isEqualTo(88131L);
        assertThat(row.get("campaign_id")).isNull();
        assertThat(row.get("member_id")).isNull();
        assertThat(row.get("coupon_id")).isNull();
    }

    @Test
    @DisplayName("회차 검출은 campaign_id 에 들어간다 — 레거시 컬럼명이 회차를 가리킨다")
    void mapCouponToLegacyCouponRoundColumn() {
        adapter.appendAll(runId, List.of(VerificationFinding.forCoupon(
                FindingType.STOCK_MISMATCH, 812, "active_count=9998", "집계=10001")));

        Map<String, Object> row = findByTargetKey("COUPON:812");
        assertThat(row.get("campaign_id")).isEqualTo(812L);
        assertThat(row.get("coupon_id")).isNull();
    }

    @Test
    @DisplayName("발급건 검출은 coupon_id 에 들어간다 — 레거시 컬럼명이 발급건을 가리킨다")
    void mapIssuanceToLegacyCouponColumn() {
        adapter.appendAll(runId, List.of(VerificationFinding.forIssuance(
                FindingType.REPLAY_MISMATCH, 44210, "replay=USED", "status=ISSUED")));

        Map<String, Object> row = findByTargetKey("ISSUANCE:44210");
        assertThat(row.get("coupon_id")).isEqualTo(44210L);
        assertThat(row.get("campaign_id")).isNull();
    }

    @Test
    @DisplayName("회차·회원 검출은 두 컬럼을 함께 채운다")
    void fillCouponAndMemberColumns() {
        adapter.appendAll(runId, List.of(VerificationFinding.forCouponMember(
                FindingType.DUP_PER_MEMBER, 812, 9931, "1건", "2건")));

        Map<String, Object> row = findByTargetKey("COUPON:812|MEMBER:9931");
        assertThat(row.get("campaign_id")).isEqualTo(812L);
        assertThat(row.get("member_id")).isEqualTo(9931L);
    }

    @Test
    @DisplayName("같은 검출을 다시 쌓아도 죽지 않고 한 행으로 남는다 — 청크가 죽은 지점부터 다시 돈다")
    void rewriteOnRestart() {
        adapter.appendAll(runId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131, "a", "b")));
        adapter.appendAll(runId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131, "c", "d")));

        assertThat(countOf(runId)).isEqualTo(1);
        assertThat(findByTargetKey("HISTORY:88131").get("expected")).isEqualTo("c");
    }

    @Test
    @DisplayName("같은 대상이라도 규칙이 다르면 다른 행이다 — 유형 3 이 V1 과 V4 를 함께 울린다")
    void keepSeparateRowsPerFindingType() {
        adapter.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 812, "a", "b"),
                VerificationFinding.forCouponMember(FindingType.DUP_PER_MEMBER, 812, 9931, "c", "d")));

        assertThat(countOf(runId)).isEqualTo(2);
    }

    @Test
    @DisplayName("다른 run 의 같은 검출은 따로 쌓인다 — run 마다 네임스페이스가 갈린다")
    void keepFindingsPerRun() {
        long otherRunId = newRun(2);

        adapter.appendAll(runId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131, "a", "b")));
        adapter.appendAll(otherRunId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131, "a", "b")));

        assertThat(countOf(runId)).isEqualTo(1);
        assertThat(countOf(otherRunId)).isEqualTo(1);
    }

    @Test
    @DisplayName("빈 목록은 아무것도 쓰지 않는다")
    void appendNothingForEmptyList() {
        adapter.appendAll(runId, List.of());

        assertThat(countOf(runId)).isZero();
    }

    @Test
    @DisplayName("한 묶음에 800행을 넣어도 다 들어간다 — 오염셋 정답이 그 규모다")
    void appendCorruptSetSizedBatch() {
        List<VerificationFinding> findings = LongStream.rangeClosed(1, 800)
                .mapToObj(id -> VerificationFinding.forHistory(
                        FindingType.ILLEGAL_TRANSITION, id, "a", "b"))
                .toList();

        adapter.appendAll(runId, findings);

        assertThat(countOf(runId)).isEqualTo(800);
    }

    /**
     * <b>800행은 분할 경계를 넘지 않는다.</b> {@code BATCH_SIZE} 가 1000 이라 위 테스트는
     * 루프를 <b>한 번만</b> 돌리고 끝난다 — 두 번째 묶음의 오프셋 계산은 한 번도 실행된 적이 없었다.
     *
     * <p>규칙당 상한 기본값이 10000 이라 실제 실행은 경계를 쉽게 넘는다.
     */
    @Test
    @DisplayName("분할 경계를 넘겨도 다 들어간다 — 두 번째 묶음이 실제로 돈다")
    void appendAcrossBatchBoundary() {
        int size = 1_001;
        List<VerificationFinding> findings = LongStream.rangeClosed(1, size)
                .mapToObj(id -> VerificationFinding.forHistory(
                        FindingType.ILLEGAL_TRANSITION, id, "a", "b"))
                .toList();

        adapter.appendAll(runId, findings);

        assertThat(keysOf(runId))
                .as("건수만 보면 누락과 어긋난 키가 상쇄돼 통과한다 — 이 PR 이 판정에서 버린 바로 그 논리다")
                .containsExactlyInAnyOrderElementsOf(LongStream.rangeClosed(1, size)
                        .mapToObj(id -> FindingType.ILLEGAL_TRANSITION + ":HISTORY:" + id)
                        .toList());
    }

    /**
     * <b>제출용 리포트가 읽는 축이다.</b> 판정의 검출 수와 이 합계가 갈리면
     * {@code finding_type} 에 규칙 목록 밖의 값이 들어갔다는 뜻이고, 그때 봐야 할 것은
     * 리포트가 아니라 규칙 쪽이다.
     */
    @Test
    @DisplayName("규칙별로 세면 합계가 전체 검출 수와 같다")
    void countByTypeSumsToTotal() {
        adapter.appendAll(runId, List.of(
                VerificationFinding.forHistory(FindingType.ILLEGAL_TRANSITION, 1, "a", "b"),
                VerificationFinding.forHistory(FindingType.ILLEGAL_TRANSITION, 2, "a", "b"),
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 3, "a", "b")));

        Map<FindingType, Integer> byType = adapter.countByType(runId);

        assertThat(byType)
                .containsEntry(FindingType.ILLEGAL_TRANSITION, 2)
                .containsEntry(FindingType.STOCK_MISMATCH, 1);
        assertThat(byType.values().stream().mapToInt(Integer::intValue).sum())
                .as("합계가 countOf 와 달라지면 전이표 밖의 finding_type 이 들어간 것이다")
                .isEqualTo(adapter.countOf(runId));
    }

    /**
     * <b>검출이 0인 규칙은 여기서 안 나온다.</b> {@code GROUP BY} 가 없는 것을 못 만들어서다 —
     * 여섯 규칙을 다 보여 주는 것은 {@code VerifyReportView} 의 몫이고, 그 경계를 여기 못 박는다.
     */
    @Test
    @DisplayName("검출이 없는 규칙은 결과에 없다 — 채우는 것은 저장소 일이 아니다")
    void omitsRulesWithoutFindings() {
        adapter.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 3, "a", "b")));

        assertThat(adapter.countByType(runId))
                .hasSize(1)
                .containsOnlyKeys(FindingType.STOCK_MISMATCH);
    }

    /**
     * <b>{@code finding_type} 에 CHECK 제약이 없다</b>({@code varchar(40)} + 주석뿐).
     * 규칙을 하나 더해 행을 쓴 뒤 코드를 되돌리면 이 상태가 된다.
     *
     * <p>{@code valueOf} 를 그냥 부르면 {@code IllegalArgumentException} 이 올라가고,
     * <b>제출물 조회가 500 + 스프링 기본 본문</b>으로 끝난다 — 원인이 어디에도 안 남아
     * 판정을 아예 못 읽는다. 도메인 예외로 바꿔 봉투에 코드를 싣는다.
     *
     * <p><b>조용히 건너뛰면 안 된다.</b> 그러면 규칙별 검출 수가 실제보다 적어지고,
     * 그 리포트가 합격 증거로 쓰인다.
     */
    @Test
    @DisplayName("모르는 규칙 이름이 섞이면 도메인 예외다 — 500 으로 죽으면 원인이 안 남는다")
    void rejectsUnknownFindingType() {
        jdbcClient.sql("""
                        INSERT INTO verification_findings
                                    (run_id, finding_type, target_key, expected, actual)
                        VALUES (:runId, 'V7_FROM_THE_FUTURE', 'COUPON:1', 'e', 'a')
                        """)
                .param("runId", runId)
                .update();

        assertThatThrownBy(() -> adapter.countByType(runId))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("V7_FROM_THE_FUTURE");
    }

    @Test
    @DisplayName("남의 실행 검출은 안 센다")
    void countsOnlyThisRun() {
        long other = newRun(2);
        adapter.appendAll(other, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 3, "a", "b")));

        assertThat(adapter.countByType(runId))
                .as("run_id 조건이 빠지면 제출물이 남의 판정을 싣는다")
                .isEmpty();
    }

    private Map<String, Object> findByTargetKey(String targetKey) {
        return jdbcClient.sql("""
                        SELECT finding_type, target_key, campaign_id, member_id,
                               coupon_id, history_id, expected, actual
                          FROM verification_findings
                         WHERE run_id = :runId AND target_key = :targetKey
                        """)
                .param("runId", runId)
                .param("targetKey", targetKey)
                .query()
                .singleRow();
    }

    /**
     * 검출을 <b>{@code (finding_type, target_key)} 쌍</b>으로 전부. 건수가 아니라 집합을 본다.
     *
     * <p>키가 쌍인 것은 이 저장소 전체의 어휘다 — {@code uk_run_finding} 도, checksum 입력도,
     * 정답 매니페스트 조인도 그 쌍이다. {@code target_key} 만 보면 종류가 틀려도 통과한다.
     * 표기는 {@code ExpectedFindingJdbcAdapterTest} 와 같은 {@code FindingKey#toString} 이다.
     */
    private List<String> keysOf(long targetRunId) {
        return jdbcClient.sql("""
                        SELECT finding_type, target_key
                          FROM verification_findings WHERE run_id = :runId
                        """)
                .param("runId", targetRunId)
                .query((rs, rowNum) ->
                        rs.getString("finding_type") + ":" + rs.getString("target_key"))
                .list();
    }

    private int countOf(long targetRunId) {
        return jdbcClient.sql("SELECT COUNT(*) FROM verification_findings WHERE run_id = :runId")
                .param("runId", targetRunId)
                .query(Integer.class)
                .single();
    }
}
