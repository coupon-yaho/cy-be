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
     * <p><b>그 "필요해지는 날" 이 왔다</b>(CY-590 의 제출용 리포트). 한때 이 값을 읽는 코드가
     * 없어 {@code VerificationRun} 레코드에 안 넣었는데, 리포트가 <i>"어느 묶음과 대조해
     * 통과했나"</i> 를 응답에 실어야 해서 레코드로 올렸다.
     *
     * <p><b>쓰기는 여전히 이 메서드가 전담한다.</b> {@code INSERT}·{@code UPDATE} 어느 쪽도
     * 이 컬럼을 안 건드리므로 덮어쓰기 사고가 없다. 읽기는 {@code restore} 의 마지막 인자다.
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

    /**
     * 이 조합에서 <b>가장 최근에 닫힌</b> 실행. 없으면 빈 값이다.
     *
     * <p><b>지표가 이것을 읽는다.</b> 판정을 잡 실행 중에 게이지로 밀어 넣는 방식이 아니라,
     * 주기적으로 이 행을 되읽어 채운다. 만료 배치는 5분 크론이라 프로세스 게이지가 재시작
     * 뒤 곧 복구되지만, 검증은 <b>사람이 손으로, 드물게</b> 돌린다 — 컨테이너를 재배포하면
     * 판정이 지표에서 사라지는데 이 테이블에는 남아 있어 <b>관제와 진실이 갈린다.</b>
     * 금요일 {@code FAIL} 이 주말 재시작으로 없어지는 모양이다.
     *
     * <p><b>닫힌 것만 본다.</b> {@code finished_at} 이 없는 행은 돌다 말았거나 지금 도는
     * 중이라 판정이 아니다. 그것을 섞으면 <i>"판정이 없다"</i> 와 <i>"아직 안 끝났다"</i> 가
     * 한 값으로 뭉친다.
     */
    Optional<VerificationRun> findLatestClosed(DatasetType dataset, ScopeType scope);

    /**
     * 같은 {@code (asOf, dataset, scope)} 에서 <b>마지막으로 쓰인 attempt + 1</b>.
     * 하나도 없으면 1 입니다. <b>중간의 빈 번호는 재사용하지 않습니다</b> — 번호를
     * 시간순으로 읽을 수 있게 두는 편이 낫습니다.
     *
     * <p><b>두 소스를 함께 봅니다.</b> {@code verification_runs} 행은 가드를 통과한 뒤에야
     * 생기는데 배치 메타(`BATCH_JOB_INSTANCE`)는 시작 즉시 생깁니다. 앞만 보면 가드에 걸려
     * 죽은 번호를 다시 줘서 <b>같은 요청이 영원히 거절</b>됩니다({@code preventRestart}).
     * 메타 쪽은 {@code verifyJob} 실행만 셉니다 — 잡 이름을 안 보면 다른 잡이 같은 이름의
     * 파라미터를 쓰는 날 번호가 뒤섞입니다.
     *
     * <p>{@code uk_run_params} 가 넷을 묶어 유일성을 걸므로 재실행에는 새 {@code attempt} 가
     * 필요한데, <b>시드가 앞 번호를 점유한다</b> — CLEAN 은 1·2, CORRUPT 는 1. 사람이 그것을
     * 외우고 있어야 한다면 트리거 API 가 매번 {@code INVALID_RUN_PARAMS} 로 죽는다.
     *
     * <p>이 값은 <b>제안일 뿐 보증이 아니다.</b> 조회와 INSERT 사이에 다른 실행이 끼면
     * 여전히 유니크 위반이 나는데, 그때 막는 것은 이 메서드가 아니라 인덱스다 — 판정을
     * 애플리케이션으로 옮기지 않는다.
     */
    int nextAttempt(LocalDateTime asOf, DatasetType dataset, ScopeType scope);
}
