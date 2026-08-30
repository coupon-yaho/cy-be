// 검증 잡과 되읽기·스케줄러가 함께 아는 계약입니다.
package com.kafkick.batch.config;

/**
 * <b>검증 잡 · 크론 스케줄러 · 지표 되읽기가 함께 아는 계약.</b> 잡 이름 하나와 SLA 가 보는
 * {@code (dataset, scope)} 조합뿐이다.
 *
 * <h2>왜 {@code VerifyJobConfig} 에 두지 않나</h2>
 *
 * <p>{@code BatchRunMetricsRefresher} 가 이 셋을 쓴다. 그것을 잡 설정에서 직접 가져오면
 * <b>{@code batch.config → batch.job} 화살표가 생기는데, 그 방향은 이 저장소에 없다</b> —
 * {@code BatchStructuralContractTest} 가 그 자물쇠다. CY-421 이 실제로 그 화살표를 하나
 * 만들었고 리뷰가 잡아 {@link ExpireStepContext} 로 계약을 내렸다. 이 클래스는 검증 쪽의
 * 같은 자리다.
 *
 * <p><b>{@code VerifyScheduler} 도 여기서 받는다.</b> 스케줄러가 도는 조합과 게이지가 내는
 * 조합은 <b>반드시 같아야 하는데</b>, 각자 자기 상수를 들면 한쪽만 바꾸는 실수를 아무것도
 * 안 막는다 — 그때 게이지는 영원히 비어 있고 {@code VerifyNeverSucceeded} 가 배포 첫날부터
 * 운다. 값이 하나면 그 실수가 생길 자리가 없다.
 */
public final class VerifyRunContext {

    /** 크론·되읽기·정리 가드가 모두 이 이름으로 배치 메타를 조회한다. */
    public static final String JOB_NAME = "verifyJob";

    /**
     * <b>SLA 가 보는 조합.</b> {@code verifyJob} 하나가 {@code CLEAN/FULL}(게이트가 보는 것)과
     * {@code CORRUPT/FULL}(리허설)을 함께 도는데, 잡 이름 그레인으로 SLA 를 걸면 <b>리허설
     * 한 번이 시계열을 앞으로 밀어 SLA 를 리셋한다</b> — 정작 게이트가 보는 조합은 며칠째
     * 안 돌았는데 조용하다.
     *
     * <p><b>{@code CORRUPT} 를 여기 더하면 안 된다.</b> 오염셋 검증은 크론이 없어 <b>안 도는
     * 것이 정상</b>이라, 그 조합에 SLA 를 걸면 영구 발화한다 — {@code verifyJob} 에 SLA 를
     * 못 걸었던 원래 이유가 그것이다.
     */
    public static final String SLA_DATASET = "CLEAN";

    /**
     * {@code INCREMENTAL} 은 {@code rejectUnsupportedScope} 가 시작 전에 거부해 닫힌 실행이
     * 생길 수 없다 — 조합에 넣어도 언제나 빈 값이다.
     */
    public static final String SLA_SCOPE = "FULL";

    private VerifyRunContext() {
    }
}
