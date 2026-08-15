package com.kafkick.core.verification;

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
}
