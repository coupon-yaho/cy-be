package com.kafkick.core.verification;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * 검증 실행 기록의 저장 계약.
 *
 * <p>batch 모듈은 storage 를 runtimeOnly 로만 보므로 JpaRepository 를 직접 참조할 수 없다.
 * 계약은 여기 두고 어댑터는 storage 에 둔다.
 */
public interface VerificationRunRepository {

    /** 실행을 기록하고 식별자가 채워진 것을 돌려준다. asof_state 가 이 id 를 FK 로 문다. */
    VerificationRun save(VerificationRun run);

    /**
     * 실행 행 <b>전체</b>를 덮어쓴다. 읽어 온 행 위에서만 부르십시오 —
     * 새로 만든 객체로 부르면 판정·checksum·지문이 함께 NULL 로 밀립니다.
     */
    void update(VerificationRun run);

    /**
     * 통계 상태만 바꿉니다. 통계 Step 이 {@link #update} 를 쓰면 <b>판정을 지울 수 있어서</b>
     * 따로 둡니다 — {@code finalizeRunStep} 이 채운 네 컬럼이 통계 갱신 한 번에 사라지면,
     * {@code verdict IS NULL} 이 "실행이 실패했다" 는 신호로 쓰이는 이 저장소에서
     * 성공한 실행이 실패로 보입니다.
     */
    void updateStatsStatus(long runId, StatsStatus status);

    /**
     * <b>대조한 정답 묶음을 실행 행에 남긴다.</b> CORRUPT 전용이다.
     *
     * <p>{@code VerificationRun} 레코드에 넣지 않았다. 이 값을 읽는 코드가 없고 — 게이트는
     * {@code verdict} 를 읽는다 — 사람이 나중에 <i>"어느 묶음과 대조해 통과했나"</i> 를 조회하는
     * 증적이다. 레코드에 넣으면 {@code start}·{@code restore}·{@code finish} 와 <b>그 호출자
     * 전부</b>가 {@code null} 을 실어 나르게 된다 — 대부분 테스트다. 필요해지는 날 그때 올린다.
     *
     * <p>수치를 적지 않는다. 호출자 수는 테스트가 늘 때마다 바뀌어 <b>적는 순간 낡는다.</b>
     */
    void recordComparedManifest(long runId, long seedRunId);

    Optional<VerificationRun> findById(long id);

    /**
     * 식별 파라미터로 실행을 되찾는다. {@code uk_run_params} 가 이 조합에 걸려 있어 많아야 하나다.
     *
     * <p>재시작 때 필요하다. 실행 기록 Step 이 COMPLETED 로 커밋됐는데 잡 컨텍스트가 아직
     * 저장되기 전에 프로세스가 죽으면, 재시작 시 그 Step 은 건너뛰는데 실행 식별자는 없다.
     * 다시 INSERT 하면 이번엔 중복키에 걸린다. 찾을 수 있어야 두 방향이 다 풀린다.
     */
    Optional<VerificationRun> findByParams(
            LocalDateTime asOf, DatasetType dataset, ScopeType scope, int attempt);
}
