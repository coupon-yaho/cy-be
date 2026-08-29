// 컨텍스트가 뜰 때 앱 표를 비웁니다. CLEAN·CORRUPT 두 컨테이너가 함께 씁니다.
package com.kafkick.storage.db;

import java.util.List;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.testcontainers.mysql.MySQLContainer;

/**
 * <b>"컨텍스트 하나 = 빈 DB 하나" 를 되살린다.</b>
 *
 * <p>{@link SharedMySqlContainers} 가 컨테이너를 JVM 소유로 바꾸면서 그 성질이 사라졌다.
 * 배치 쪽 테스트는 {@code VerificationSeed#clear} 로 스스로 비우지만, 합류로 들어온 다른
 * 영역의 테스트들은 그 규약을 채택한 적이 없다 — 커밋하는 테스트가 남긴 행
 * ({@code brands} id=1, {@code grades} 'GOLD')과 부딪혀 <b>다음 클래스가 중복키로 죽었다</b>
 * (실측: storage 48건). 그래서 비우는 책임을 테스트가 아니라 인프라가 진다.
 *
 * <p><b>CLEAN 과 CORRUPT 둘 다 필요하다.</b> 처음에 CLEAN 에만 달았다가 리뷰가 잡았다 —
 * CORRUPT 컨테이너도 JVM 공유이고, 그쪽 테스트가 세는 것은 <b>검출 건수</b>다.
 * 남의 컨텍스트가 남긴 {@code verification_findings}·{@code expected_findings} 행이
 * 그 계수에 섞이면 누락·오탐 건수가 <b>실행 순서에 따라 달라진다.</b>
 *
 * <p>⚠️ <b>테스트가 직렬로 도는 것을 전제한다.</b> 루트 {@code build.gradle} 이
 * {@code maxParallelForks}·{@code forkEvery} 를 안 켜는 이유를 그 파일이 적고 있다.
 * 병렬로 바꾸는 날 이 비우기가 남의 컨텍스트를 지우므로, 그때는 스키마를 갈라야 한다.
 *
 * <p>⚠️ <b>배치 메타({@code BATCH_*})는 안 건드린다.</b> 그쪽은 {@code BatchMetadata#clear}
 * 가 지고, 여기서 함께 지우면 잡을 돌리는 테스트가 서로의 실행 이력을 지운다.
 *
 * <p>⚠️ <b>{@code coupon_round_schedule_guard} 도 안 건드린다.</b> 그 표는 마이그레이션이
 * 싱글턴 행을 넣고 Flyway 는 다시 안 돈다 — 지우면 되돌릴 방법이 없다.
 */
public final class AppTableCleaner {

    /**
     * FK 역순. {@code VerificationSeed.TABLES_IN_DELETE_ORDER} 와 같은 순서다 —
     * 그쪽은 배치 테스트가 쓰고 이쪽은 컨텍스트 기동이 쓴다.
     *
     * <p>관측·부하측정 표({@code analytics_*}·{@code benchmark_runs}·{@code run_timeseries}·
     * {@code consistency_finals}·{@code issue_attempts})는 아직 없다. 그 표를 읽는 테스트가
     * 이 컨테이너로 옮겨 오면 그때 더한다 — 지금 넣으면 지울 것이 없는 DELETE 가 돈다.
     */
    private static final List<String> TABLES_IN_DELETE_ORDER = List.of(
            "hourly_stats", "grade_stats", "coupon_stats",
            "asof_state", "verification_findings", "expected_findings", "verification_runs",
            "idempotency_records",
            "issuance_usages", "issuance_histories", "issuances",
            "coupon_stocks", "coupons", "coupon_templates", "brands",
            "members", "grades");

    private AppTableCleaner() {
    }

    public static SmartInitializingSingleton of(MySQLContainer container) {
        return () -> MySqlContainerConfig.executeAsRoot(container, "SET FOREIGN_KEY_CHECKS = 0;"
                + TABLES_IN_DELETE_ORDER.stream()
                        .map(table -> "TRUNCATE TABLE " + container.getDatabaseName()
                                + "." + table + ";")
                        .reduce("", String::concat)
                + "SET FOREIGN_KEY_CHECKS = 1;");
    }
}
