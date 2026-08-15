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

    /** 판정·통계 상태·증적을 갱신한다. */
    void update(VerificationRun run);

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
