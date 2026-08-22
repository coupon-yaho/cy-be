package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.exception.VerificationErrorCode;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
@Import(VerificationRunJdbcAdapter.class)
class VerificationRunJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);
    private static final LocalDateTime STARTED_AT = LocalDateTime.of(2026, 8, 15, 14, 0, 1);

    @Autowired
    private VerificationRunJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    @DisplayName("실행을 저장하면 식별자가 채워진다 — asof_state 가 이 값을 FK 로 문다")
    void saveAssignsId() {
        VerificationRun saved = adapter.save(fullRun());

        assertThat(saved.id()).isNotNull().isPositive();
    }

    @Test
    @DisplayName("저장한 실행을 그대로 다시 읽는다")
    void findSavedRun() {
        VerificationRun saved = adapter.save(fullRun());

        Optional<VerificationRun> found = adapter.findById(saved.id());

        assertThat(found).isPresent();
        assertThat(found.get().asOf()).isEqualTo(AS_OF);
        assertThat(found.get().scope()).isEqualTo(ScopeType.FULL);
        assertThat(found.get().dataset()).isEqualTo(DatasetType.CLEAN);
        assertThat(found.get().attempt()).isEqualTo(1);
        assertThat(found.get().verdict()).isNull();
        assertThat(found.get().finishedAt()).isNull();
    }

    @Test
    @DisplayName("증분 실행의 시작 시각이 왕복해도 보존된다")
    void keepFromTsThroughRoundTrip() {
        LocalDateTime fromTs = AS_OF.minusMinutes(10);
        VerificationRun saved = adapter.save(VerificationRun.start(
                AS_OF, fromTs, ScopeType.INCREMENTAL, DatasetType.CLEAN, 1, STARTED_AT));

        Optional<VerificationRun> found = adapter.findById(saved.id());

        assertThat(found).isPresent();
        assertThat(found.get().fromTs()).isEqualTo(fromTs);
    }

    @Test
    @DisplayName("판정과 증적을 갱신하면 조회에 반영된다")
    void updateVerdictAndEvidence() {
        VerificationRun saved = adapter.save(fullRun());

        adapter.update(saved.finish(
                VerdictType.FAIL, 3, "c".repeat(64), "f".repeat(64),
                LocalDateTime.of(2026, 8, 15, 14, 3, 4)));

        VerificationRun found = adapter.findById(saved.id()).orElseThrow();
        assertThat(found.verdict()).isEqualTo(VerdictType.FAIL);
        assertThat(found.findingCount()).isEqualTo(3);
        assertThat(found.findingsChecksum()).isEqualTo("c".repeat(64));
        assertThat(found.datasetFingerprint()).isEqualTo("f".repeat(64));
        assertThat(found.finishedAt()).isNotNull();
    }

    @Test
    @DisplayName("판정과 통계 상태를 한 번의 update 로 함께 쓴다")
    void writeVerdictAndStatsStatusTogether() {
        VerificationRun saved = adapter.save(fullRun());

        adapter.update(saved.finish(VerdictType.PASS, 0, null, null, STARTED_AT)
                .withStatsStatus(StatsStatus.SKIPPED));

        VerificationRun found = adapter.findById(saved.id()).orElseThrow();
        assertThat(found.verdict()).isEqualTo(VerdictType.PASS);
        assertThat(found.statsStatus()).isEqualTo(StatsStatus.SKIPPED);
    }

    /**
     * <b>이름이 말하는 메서드를 실제로 부른다.</b> 위 테스트가 {@code updateStatsStatus} 라는
     * 이름을 달고 {@code update()} 만 불러, 통계 Step 이 쓸 전용 경로는 한 번도 안 돌았다.
     */
    @Test
    @DisplayName("통계 상태만 따로 갱신한다 — 판정은 건드리지 않는다")
    void updateStatsStatusAlone() {
        VerificationRun saved = adapter.save(fullRun());
        adapter.update(saved.finish(VerdictType.PASS, 0, null, null, STARTED_AT));

        adapter.updateStatsStatus(saved.id(), StatsStatus.COMPLETE);

        VerificationRun found = adapter.findById(saved.id()).orElseThrow();
        assertThat(found.statsStatus()).isEqualTo(StatsStatus.COMPLETE);
        assertThat(found.verdict())
                .as("통계 갱신이 판정을 지우면 게이트가 읽을 값이 사라진다")
                .isEqualTo(VerdictType.PASS);
    }

    /**
     * <b>조용히 넘어가면 판정이 안 써진 채 잡이 COMPLETED 로 끝난다.</b> 게이트가 읽는 것은
     * {@code verification_runs.verdict} 라, 그 행이 NULL 이면 판정 불가가 된다.
     *
     * <p>코드가 <b>500대</b>인 것도 함께 지킨다. 404 로 두면 클라이언트 입력 오류로 분류돼
     * 재고 소진 같은 정상 흐름 예외와 같은 취급을 받는다 — 이건 데이터 정합 사고다.
     */
    @Test
    @DisplayName("지워진 실행을 갱신하면 RUN_ROW_VANISHED 로 죽는다")
    void rejectUpdateOfDeletedRun() {
        VerificationRun saved = adapter.save(fullRun());
        jdbcClient.sql("DELETE FROM verification_runs WHERE id = :id")
                .param("id", saved.id())
                .update();

        assertThatThrownBy(() -> adapter.update(
                saved.finish(VerdictType.PASS, 0, null, null, STARTED_AT)))
                .isInstanceOf(BusinessException.class)
                .extracting(e -> ((BusinessException) e).getErrorCode())
                .isEqualTo(VerificationErrorCode.RUN_ROW_VANISHED);
    }

    @Test
    @DisplayName("없는 실행의 통계 상태를 갱신해도 죽는다 — 조용히 넘어가면 통계가 빈 채 끝난다")
    void rejectStatsUpdateOfMissingRun() {
        assertThatThrownBy(() -> adapter.updateStatsStatus(999_999L, StatsStatus.SKIPPED))
                .isInstanceOf(BusinessException.class)
                .extracting(e -> ((BusinessException) e).getErrorCode())
                .isEqualTo(VerificationErrorCode.RUN_ROW_VANISHED);
    }

    /**
     * <b>PASS 행 하나로 "어느 묶음과 대조했나" 에 답할 수 있어야 한다.</b> 그 값이 잡 실행
     * 컨텍스트에만 있으면 정리 배치가 한 번 돌면 영영 못 답한다.
     */
    @Test
    @DisplayName("대조한 정답 묶음을 실행 행에 남긴다 — 판정은 건드리지 않는다")
    void recordComparedManifest() {
        // CORRUPT 여야 한다. ck_seed_run_id_corrupt_only 가 CLEAN 행에 이 값을 못 넣게 막는다.
        VerificationRun saved = adapter.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CORRUPT, 1, STARTED_AT));
        adapter.update(saved.finish(VerdictType.PASS, 0, null, null, STARTED_AT));

        adapter.recordComparedManifest(saved.id(), 7L);

        assertThat(jdbcClient.sql("SELECT seed_run_id FROM verification_runs WHERE id = :id")
                .param("id", saved.id())
                .query(Long.class)
                .single())
                .isEqualTo(7L);
        assertThat(adapter.findById(saved.id()).orElseThrow().verdict())
                .as("증적을 남기다가 판정을 지우면 게이트가 읽을 값이 사라진다")
                .isEqualTo(VerdictType.PASS);
    }

    /**
     * <b>불변식을 DB 제약으로 표현한다</b>(설계 원칙 1번). CLEAN 은 대조할 묶음이 없으므로
     * 이 컬럼이 채워진 CLEAN 행은 앞뒤가 안 맞는다. 방어가 호출자 분기 한 곳뿐이면
     * 다른 경로가 생기는 날 조용히 뚫린다.
     */
    @Test
    @DisplayName("CLEAN 실행에는 정답 묶음을 못 남긴다 — DB 가 막는다")
    void rejectManifestRecordOnCleanRun() {
        VerificationRun clean = adapter.save(fullRun());

        // 예외 형이 아니라 제약 이름을 단정한다. MySQL 의 CHECK 위반은 3819/HY000 인데
        // Spring 의 SQLState 번역기가 그 조합을 모르는 예외로 넘겨, 형으로 고정하면
        // 무엇이 막았는지가 아니라 번역기의 사정을 단정하게 된다.
        assertThatThrownBy(() -> adapter.recordComparedManifest(clean.id(), 1L))
                .as("CLEAN 은 검출 0건이 통과 조건이라 대조 상대가 없다")
                .hasMessageContaining("ck_seed_run_id_corrupt_only");
    }

    @Test
    @DisplayName("없는 실행에 묶음을 기록하면 죽는다 — 조용히 넘어가면 증적이 빈다")
    void rejectManifestRecordOfMissingRun() {
        assertThatThrownBy(() -> adapter.recordComparedManifest(999_999L, 1L))
                .isInstanceOf(BusinessException.class)
                .extracting(e -> ((BusinessException) e).getErrorCode())
                .isEqualTo(VerificationErrorCode.RUN_ROW_VANISHED);
    }

    @Test
    @DisplayName("없는 실행을 찾으면 빈 값이다")
    void findMissingRun() {
        assertThat(adapter.findById(999_999L)).isEmpty();
    }

    @Test
    @DisplayName("식별자 없이 갱신하면 거부한다")
    void rejectUpdateWithoutId() {
        assertThatThrownBy(() -> adapter.update(fullRun()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("갱신하려면 검증 실행 ID가 필요합니다.");
    }

    @Test
    @DisplayName("같은 파라미터로 두 번 저장하면 uk_run_params 가 막는다 — attempt 를 올려야 재실행된다")
    void rejectDuplicateRunParams() {
        adapter.save(fullRun());

        assertThatThrownBy(() -> adapter.save(fullRun()))
                .isInstanceOf(org.springframework.dao.DuplicateKeyException.class);
    }

    @Test
    @DisplayName("attempt 를 올리면 같은 asOf 로 다시 실행된다 — 결정론 증명의 전제다")
    void allowSameAsOfWithNextAttempt() {
        adapter.save(fullRun());

        VerificationRun second = adapter.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 2, STARTED_AT));

        assertThat(second.id()).isNotNull();
        assertThat(second.attempt()).isEqualTo(2);
    }

    @Test
    @DisplayName("asof_state 가 실행을 FK 로 문다 — 실행 행이 먼저 있어야 한다")
    void asofStateReferencesRun() {
        VerificationRun saved = adapter.save(fullRun());

        int inserted = jdbcClient.sql("""
                        INSERT INTO asof_state (run_id, coupon_id, state, active_usage_count)
                        VALUES (:runId, 1, 'ISSUED', 0)
                        """)
                .param("runId", saved.id())
                .update();

        assertThat(inserted).isEqualTo(1);
    }

    /**
     * <b>PRD 의 뷰 정의는 시드가 만드는 상황에서 비결정적이다.</b> {@code PRD-v4.15.md:953} 이
     * {@code ORDER BY as_of DESC LIMIT 1} 로 적었는데, 시드는 CLEAN 을 <b>같은 {@code as_of}</b> 에
     * {@code attempt} 1·2 로 심고 둘 다 {@code stats_status = COMPLETE} 다(결정론 증명용).
     *
     * <p><b>{@code id} 가 아니라 {@code attempt} 로 정렬한다.</b> 계약이 말하는 것은 "나중 시도"
     * 인데 {@code id} 는 삽입 순서다. 둘을 맞추는 제약이 없어서, 결정론 증명을 {@code attempt}
     * 3 → 4 → 2 순서로 돌리면 {@code id} 는 4·5·6 이라 계약이 말한 4 가 아니라 2 를 고른다.
     *
     * <p>그래서 여기서는 <b>{@code id} 와 {@code attempt} 를 일부러 어긋나게</b> 넣는다 —
     * 나중 시도({@code attempt 2})를 <b>먼저</b> 저장해 작은 {@code id} 를 받게 한다.
     * 이러면 {@code attempt DESC} 를 지우는 돌연변이가 빨개진다({@code id DESC} 만 남으면
     * {@code attempt 1} 을 고른다).
     */
    @Test
    @DisplayName("최신 통계 뷰는 같은 as_of 면 나중 시도를 고른다 — id 가 아니라 attempt 다")
    void pickLaterAttemptOnSameAsOf() {
        long laterAttempt = completedCleanRun(2);
        long earlierAttempt = completedCleanRun(1);

        assertThat(earlierAttempt)
                .as("id 와 attempt 가 어긋나야 이 테스트가 무언가를 지킨다")
                .isGreaterThan(laterAttempt);
        assertThat(latestStatsRun()).isEqualTo(laterAttempt);
    }

    /**
     * <b>증분은 윈도우 집계라 대시보드의 분모가 될 수 없다.</b> {@code (from_ts, as_of]} 로 잘린
     * 하루치가 누적 발급률의 분모로 들어가면 화면 숫자가 조용히 부풀어 오른다.
     *
     * <p>지금은 {@code rejectUnsupportedScope} 가 증분을 막지만, 증분 티켓이 그 가드를 푸는
     * 순간 뷰에는 대응하는 장치가 없었다 — {@code finalizeRunStep} 은 그때를 대비해
     * {@code decidesVerdict} 가드를 이미 심어 뒀다.
     */
    @Test
    @DisplayName("증분 실행은 최신 통계 뷰에 들어오지 않는다")
    void excludeIncrementalFromStatsView() {
        long incremental = adapter.save(VerificationRun.start(
                AS_OF, AS_OF.minusDays(1), ScopeType.INCREMENTAL,
                DatasetType.CLEAN, 1, STARTED_AT)).id();
        adapter.update(adapter.findById(incremental).orElseThrow()
                .finish(VerdictType.PASS, 0, null, null, STARTED_AT)
                .withStatsStatus(StatsStatus.COMPLETE));

        assertThat(latestStatsRun())
                .as("전수 실행이 하나도 없으면 뷰는 비어 있어야 한다")
                .isNull();
    }

    /** 같은 {@code as_of} 에 통계까지 끝난 CLEAN 전수 실행 하나. 반환값은 그 실행 id 다. */
    private long completedCleanRun(int attempt) {
        long id = adapter.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, attempt, STARTED_AT)).id();
        adapter.update(adapter.findById(id).orElseThrow()
                .finish(VerdictType.PASS, 0, null, null, STARTED_AT)
                .withStatsStatus(StatsStatus.COMPLETE));

        return id;
    }

    /** 통계 스냅샷이 완결되지 않았으면 조회 자체가 안 돼야 한다 — 부분값을 섞어 읽지 않는다. */
    @Test
    @DisplayName("완결된 CLEAN 스냅샷이 없으면 뷰가 비어 있다")
    void hideIncompleteSnapshot() {
        adapter.update(adapter.findById(adapter.save(fullRun()).id()).orElseThrow()
                .finish(VerdictType.PASS, 0, null, null, STARTED_AT)
                .withStatsStatus(StatsStatus.PARTIAL));

        assertThat(latestStatsRun()).isNull();
    }

    private Long latestStatsRun() {
        return jdbcClient.sql("SELECT id FROM v_latest_stats_run")
                .query(Long.class)
                .optional()
                .orElse(null);
    }

    private static VerificationRun fullRun() {
        return VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 1, STARTED_AT);
    }

    /**
     * <b>{@code nextAttempt} 는 두 소스를 함께 본다.</b> {@code verification_runs} 행은 잡의
     * 가드를 통과한 뒤에야 생기는데 배치 메타는 시작 즉시 생긴다 — 앞만 보면 가드에 걸려
     * 죽은 번호를 다시 줘서 {@code preventRestart} 가 <b>같은 요청을 영원히 거절</b>한다.
     *
     * <p>여기서 각 축을 따로 잰다. HTTP 테스트 하나로는 조합 하나만 밟아
     * {@code dataset}·{@code scope} 필터가 실제로 거르는지 아무도 안 본다.
     */
    @Test
    @DisplayName("아무 실행도 없으면 1 이다")
    void startsAtOne() {
        assertThat(adapter.nextAttempt(AS_OF, DatasetType.CLEAN, ScopeType.FULL)).isEqualTo(1);
    }

    @Test
    @DisplayName("verification_runs 의 마지막 번호 다음을 준다")
    void continuesAfterTheLastPersistedRun() {
        adapter.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 3, AS_OF));

        assertThat(adapter.nextAttempt(AS_OF, DatasetType.CLEAN, ScopeType.FULL)).isEqualTo(4);
    }

    /**
     * <b>가드에 걸려 죽은 시도는 {@code verification_runs} 에 흔적이 없다.</b> 배치 메타만
     * 보고도 그 번호를 피해야 한다 — 안 그러면 같은 요청이 영원히 400 이다.
     */
    @Test
    @DisplayName("verification_runs 에 없어도 배치 메타의 번호를 피한다")
    void avoidsAttemptsThatOnlyExistInBatchMetadata() {
        insertBatchAttempt("verifyJob", AS_OF, "CLEAN", "FULL", 2);

        assertThat(adapter.nextAttempt(AS_OF, DatasetType.CLEAN, ScopeType.FULL))
                .as("이 축이 죽으면 가드에 한 번 걸린 asOf 를 다시 못 돌린다")
                .isEqualTo(3);
    }

    /**
     * <b>다른 데이터셋·scope 의 번호는 안 센다.</b> 이 필터가 죽으면 CLEAN 트리거가 CORRUPT 가
     * 태운 번호까지 피해 배정하고, {@code attempt} 열이 설명 안 되는 값으로 채워진다 —
     * 유니크 위반이 안 나서 <b>조용하다.</b>
     */
    @Test
    @DisplayName("다른 dataset·scope 의 번호는 안 센다")
    void ignoresOtherDatasetsAndScopes() {
        insertBatchAttempt("verifyJob", AS_OF, "CORRUPT", "FULL", 7);
        insertBatchAttempt("verifyJob", AS_OF, "CLEAN", "INCREMENTAL", 9);

        assertThat(adapter.nextAttempt(AS_OF, DatasetType.CLEAN, ScopeType.FULL)).isEqualTo(1);
    }

    /**
     * <b>다른 잡의 번호도 안 센다.</b> 지금 {@code expireJob} 은 {@code asOf} 하나만 써서
     * 우연히 안 걸리지만, 거기에 {@code dataset}·{@code scope}·{@code attempt} 가 붙는 날
     * 두 잡의 번호가 뒤섞인다.
     */
    /**
     * <b>다른 잡의 번호는 안 센다.</b> 이것이 어댑터의 잡 이름 상수를 지키는 축이기도 하다 —
     * storage 는 batch 를 참조할 수 없어({@code VerifyJobConfig} 는 의존 방향 반대편이다)
     * 이름을 문자열로 두었는데, 그것이 어긋나면 배치 메타 축이 조용히 0을 주고 가드에 걸려
     * 죽은 {@code attempt} 를 다시 배정한다.
     *
     * <p>지금 {@code expireJob} 은 {@code asOf} 하나만 써서 우연히 안 걸리지만, 거기에
     * {@code dataset}·{@code scope}·{@code attempt} 가 붙는 날 두 잡의 번호가 뒤섞인다.
     */
    @Test
    @DisplayName("다른 잡의 번호는 안 센다")
    void ignoresOtherJobs() {
        insertBatchAttempt("expireJob", AS_OF, "CLEAN", "FULL", 5);

        assertThat(adapter.nextAttempt(AS_OF, DatasetType.CLEAN, ScopeType.FULL)).isEqualTo(1);
    }

    /**
     * <b>초가 0 이 아닌 시각도 맞아야 한다.</b> 위 케이스들의 {@code AS_OF} 는 초가 0이라
     * {@code LocalDateTime.toString()} 이 초를 생략하는 위험한 축을 이미 밟고 있다 —
     * 문자열 비교로 되돌리면 그것들이 먼저 빨개진다. 여기서는 반대쪽을 본다.
     */
    @Test
    @DisplayName("초가 있는 asOf 도 배치 메타와 맞춘다")
    void matchesAnAsOfWithSeconds() {
        LocalDateTime withSeconds = LocalDateTime.of(2026, 8, 15, 14, 30, 45);
        insertBatchAttempt("verifyJob", withSeconds, "CLEAN", "FULL", 4);

        assertThat(adapter.nextAttempt(withSeconds, DatasetType.CLEAN, ScopeType.FULL))
                .isEqualTo(5);
    }

    /** 배치 메타에 실행 하나와 그 파라미터 넷을 심는다. 잡을 실제로 돌리지 않는다. */
    private void insertBatchAttempt(String jobName, LocalDateTime asOf,
            String dataset, String scope, int attempt) {
        long instanceId = nextMetaId("BATCH_JOB_INSTANCE", "JOB_INSTANCE_ID");
        jdbcClient.sql("""
                        INSERT INTO BATCH_JOB_INSTANCE
                          (JOB_INSTANCE_ID, VERSION, JOB_NAME, JOB_KEY)
                        VALUES (:id, 0, :jobName, :key)
                        """)
                .param("id", instanceId)
                .param("jobName", jobName)
                .param("key", "k" + instanceId)
                .update();

        long executionId = nextMetaId("BATCH_JOB_EXECUTION", "JOB_EXECUTION_ID");
        jdbcClient.sql("""
                        INSERT INTO BATCH_JOB_EXECUTION
                          (JOB_EXECUTION_ID, VERSION, JOB_INSTANCE_ID, CREATE_TIME, STATUS)
                        VALUES (:id, 0, :instanceId, :createdAt, 'FAILED')
                        """)
                .param("id", executionId)
                .param("instanceId", instanceId)
                // NOW() 를 안 쓴다. 이 저장소는 시각을 주입받는 것을 규율로 삼는다 —
                // 그 값이 이 테스트의 단언에 안 쓰이더라도, DB 함수로 현재 시각을 읽는
                // 습관이 남으면 다음 사람이 판정 경로에서도 그렇게 한다.
                .param("createdAt", asOf)
                .update();

        // 배치가 저장하는 형식 그대로 — LocalDateTime 은 ISO_LOCAL_DATE_TIME 이라 초가 항상 있다.
        param(executionId, "asOf", "java.time.LocalDateTime",
                asOf.format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        param(executionId, "dataset", "java.lang.String", dataset);
        param(executionId, "scope", "java.lang.String", scope);
        param(executionId, "attempt", "java.lang.Long", String.valueOf(attempt));
    }

    private void param(long executionId, String name, String type, String value) {
        jdbcClient.sql("""
                        INSERT INTO BATCH_JOB_EXECUTION_PARAMS
                          (JOB_EXECUTION_ID, PARAMETER_NAME, PARAMETER_TYPE, PARAMETER_VALUE, IDENTIFYING)
                        VALUES (:id, :name, :type, :value, 'Y')
                        """)
                .param("id", executionId)
                .param("name", name)
                .param("type", type)
                .param("value", value)
                .update();
    }

    private long nextMetaId(String table, String column) {
        return jdbcClient.sql("SELECT COALESCE(MAX(" + column + "), 0) + 1 FROM " + table)
                .query(Long.class)
                .single();
    }
}
