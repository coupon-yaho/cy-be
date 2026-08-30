// Spring Batch 메타 테이블을 비웁니다. 테스트 사이의 격리를 위해서입니다.
package com.kafkick.storage.db;

import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * <b>{@code JobRepositoryTestUtils.removeJobExecutions()} 만으로는 안 비워진다.</b>
 *
 * <p>그것은 잡 이름별로 인스턴스를 훑고 <b>실행이 있는 것만</b> 지운다 — 바이트코드로 확인했다
 * ({@code getJobExecutions(instance).isEmpty()} 분기에서 건너뛴다). 파라미터 검증에 걸리거나
 * {@code beforeJob} 에서 죽어 <b>인스턴스만 만들어지고 실행이 없는</b> 행은 영영 남는다.
 *
 * <p>컨테이너를 컨텍스트마다 띄우던 시절에는 무관했다. 클래스마다 빈 DB 였기 때문이다.
 * <b>지금은 배치 메타가 클래스 경계를 넘어 산다</b>({@link MySqlContainerConfig}) — 남은
 * 인스턴스가 다음 클래스의 같은 {@code asOf} 와 충돌해
 * {@code JobInstanceAlreadyCompleteException} 이 나고, 실패가 <b>실행 순서에 따라 달라진다.</b>
 *
 * <p><b>강도를 하나로 둔다.</b> 한때 이 삭제 루프가 {@code BatchJobRepositoryTest} 안에만
 * 있었는데, 그러면 <i>"왜 저기만 지우지"</i> 를 다음 사람이 판단해야 하고 판단이 틀리면
 * 순서 의존 초록이 다시 생긴다.
 */
public final class BatchMetadata {

    /**
     * FK 자식 → 부모 순서. {@code V11__batch_metadata.sql} 의 선언 순서와 대조했다 —
     * 뒤집으면 {@code DELETE} 가 외래키로 막힌다.
     */
    private static final List<String> TABLES_IN_DELETE_ORDER = List.of(
            "BATCH_STEP_EXECUTION_CONTEXT",
            "BATCH_STEP_EXECUTION",
            "BATCH_JOB_EXECUTION_CONTEXT",
            "BATCH_JOB_EXECUTION_PARAMS",
            "BATCH_JOB_EXECUTION",
            "BATCH_JOB_INSTANCE");

    private BatchMetadata() {
    }

    /** 잡을 실제로 돌리는 테스트는 {@code @BeforeEach} 에서 이것을 부른다. */
    public static void clear(JdbcClient jdbcClient) {
        TABLES_IN_DELETE_ORDER.forEach(
                table -> jdbcClient.sql("DELETE FROM " + table).update());
    }
}
