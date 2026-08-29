package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.storage.db.RepositoryTest;

/** 관제 히스토리가 쓰는 목록 조회. */
@RepositoryTest
@Import(VerificationRunJdbcAdapter.class)
class VerificationRunHistoryTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);

    @Autowired
    private VerificationRunJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    @DisplayName("최근 실행부터 준다 — as_of 가 아니라 id 순이다")
    void ordersByIdDescending() {
        long first = adapter.save(run(DatasetType.CLEAN, 1)).id();
        long second = adapter.save(run(DatasetType.CLEAN, 2)).id();

        List<VerificationRun> recent = adapter.findRecent(null, 10, 0);

        assertThat(recent).extracting(VerificationRun::id)
                .as("같은 as_of 로 여러 번 도는 재시도가 있어 as_of 로는 순서가 안 정해진다")
                .containsExactly(second, first);
    }

    @Test
    @DisplayName("dataset 을 주면 그것만, 안 주면 전체")
    void filtersByDataset() {
        adapter.save(run(DatasetType.CLEAN, 1));
        adapter.save(run(DatasetType.CORRUPT, 1));

        assertThat(adapter.findRecent(DatasetType.CORRUPT, 10, 0))
                .extracting(VerificationRun::dataset)
                .containsExactly(DatasetType.CORRUPT);
        assertThat(adapter.findRecent(null, 10, 0)).hasSize(2);
    }

    @Test
    @DisplayName("limit·offset 이 페이지를 가른다 — 겹치거나 빠지면 안 된다")
    void paginates() {
        long first = adapter.save(run(DatasetType.CLEAN, 1)).id();
        long second = adapter.save(run(DatasetType.CLEAN, 2)).id();
        long third = adapter.save(run(DatasetType.CLEAN, 3)).id();

        assertThat(adapter.findRecent(null, 2, 0)).extracting(VerificationRun::id)
                .containsExactly(third, second);
        assertThat(adapter.findRecent(null, 2, 2)).extracting(VerificationRun::id)
                .as("두 번째 페이지가 첫 페이지와 겹치면 화면이 같은 행을 두 번 그린다")
                .containsExactly(first);
    }

    @Test
    @DisplayName("건수는 필터를 함께 본다 — 화면이 마지막 페이지를 계산한다")
    void countsWithTheSameFilter() {
        adapter.save(run(DatasetType.CLEAN, 1));
        adapter.save(run(DatasetType.CLEAN, 2));
        adapter.save(run(DatasetType.CORRUPT, 1));

        assertThat(adapter.countRecent(null)).isEqualTo(3);
        assertThat(adapter.countRecent(DatasetType.CLEAN))
                .as("필터를 무시하면 화면이 없는 페이지를 그린다")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("시드가 심은 기준 행은 안 준다 — 안 거르면 안 돌리고도 PASS 가 그려진다")
    void excludesSeedPlantedRuns() {
        adapter.save(run(DatasetType.CLEAN, 1));
        plantSeedRun(DatasetType.CLEAN);

        assertThat(adapter.findRecent(null, 10, 0))
                .as("""
                        실측(2026-08-29): coupon_clean 에 origin='SEED' PASS 3행, \
                        coupon_corrupt 에 origin='SEED' FAIL 800건 1행이 실제로 있다. \
                        섞이면 CLEAN 은 배치를 한 번도 안 돌리고도 관제에 PASS 가 뜨고, \
                        CORRUPT 는 시드의 FAIL 이 배치의 PASS 옆에 떠서 집계가 틀어진다""")
                .hasSize(1);
        assertThat(adapter.countRecent(null))
                .as("items 만 거르고 total 을 안 거르면 화면이 빈 페이지를 그린다")
                .isEqualTo(1);
        assertThat(adapter.countRecent(DatasetType.CLEAN)).isEqualTo(1);
    }

    /**
     * 시드가 심는 모양 그대로 넣는다. 어댑터의 save 는 origin 기본값 'BATCH' 로만 쓰므로
     * 이 축은 직접 INSERT 해야 잴 수 있다 — 그래서 지금까지 아무 테스트도 이걸 안 봤다.
     */
    private void plantSeedRun(DatasetType dataset) {
        jdbcClient.sql("""
                INSERT INTO verification_runs
                    (as_of, scope, dataset, attempt, origin, verdict, finding_count, started_at)
                VALUES (:asOf, 'FULL', :dataset, 99, 'SEED', 'PASS', 0, :asOf)
                """).param("asOf", AS_OF).param("dataset", dataset.name()).update();
    }

    private static VerificationRun run(DatasetType dataset, int attempt) {
        return VerificationRun.start(AS_OF, null, ScopeType.FULL, dataset, attempt,
                AS_OF.plusSeconds(1));
    }
}
