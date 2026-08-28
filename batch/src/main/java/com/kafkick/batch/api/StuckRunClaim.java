// 시체 복구가 쓰는 선점 문장을 한 곳에 둡니다.
package com.kafkick.batch.api;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * <b>판정과 쓰기 사이를 닫는 선점 문장.</b> {@code requireStuck} 으로 시체라고 판정한 뒤
 * 그 조건을 <b>쓰기 문장 안에 다시 걸고</b> affected rows 를 본다 — 그 사이에 실행이
 * 되살아날 수 있고(락 대기가 풀리는 경우가 그렇다) 그때 살아 있는 잡을 닫는다.
 *
 * <p><b>왜 모아 뒀나.</b> {@link ExpireRecoveryService} 와 {@link CleanupRecoveryService} 가
 * <b>글자까지 같은 문장</b>을 쓴다. 두 벌로 두면 한쪽만 고치는 날 판정과 쓰기가 다른 조건을
 * 보게 되는데, 그 어긋남은 <b>드물게만</b> 드러나 테스트로 잡기 어렵다.
 *
 * <p><b>{@link VerifyStopService} 는 자기 것을 따로 든다.</b> 그쪽은 {@code STOPPING} 을 빼야
 * 한다 — {@code SimpleJobOperator.stop} 이 {@code STARTED}·{@code STARTING} 만 받고 나머지에
 * {@code JobExecutionNotRunningException} 을 던지므로, 넣으면 선점만 성공하고 다음 줄이
 * 던져 <b>아무것도 안 한 채 VERSION 만 올린다.</b> 그 차이가 의도라는 것을 여기 적어 둔다.
 */
final class StuckRunClaim {

    private StuckRunClaim() {
    }

    /**
     * <b>{@code STATUS} 목록이 조회와 같아야 한다</b>({@code JdbcJobExecutionDao} 의 상수와
     * 글자까지 같다). 하나라도 빠지면 그 상태의 시체가 <b>아무것도 안 한 채 200</b> 을 받는다.
     *
     * <p><b>폴백까지 그대로 옮긴다.</b> {@code RunningJobProbe} 의 판정이
     * {@code MAX(LAST_UPDATED)} → {@code START_TIME} → {@code CREATE_TIME} 순으로 떨어지는데,
     * {@code NOT EXISTS} 로만 쓰면 <b>Step 행이 없는 실행에서 무조건 참</b>이 되어 방금 뜬
     * {@code STARTING} 실행도 닫는다.
     */
    static final String CLAIM = """
            UPDATE BATCH_JOB_EXECUTION je
               SET je.VERSION = je.VERSION + 1
             WHERE je.JOB_EXECUTION_ID = :id
               AND je.STATUS IN ('STARTING','STARTED','STOPPING')
               AND COALESCE((SELECT MAX(se.LAST_UPDATED) FROM BATCH_STEP_EXECUTION se
                              WHERE se.JOB_EXECUTION_ID = je.JOB_EXECUTION_ID),
                            je.START_TIME, je.CREATE_TIME) <= :stuckBefore
            """;


    /**
     * <b>{@code abandon} 의 선점문.</b> 시체 판정이 아니라 <b>버릴 수 있는 상태</b>를 조건으로
     * 건다 — {@code abandon} 은 진도를 안 보고 {@code stop} 다음 단계를 지기 때문이다.
     *
     * <p>없으면 검사와 쓰기 사이가 열린다. {@code update(JobExecution)} 에는 낙관적 락이
     * 사실상 없으므로({@link ExpireRecoveryService} 가 적어 둔 사실) <b>동시 요청 둘이 모두
     * 통과해 {@code END_TIME} 을 두 번 쓴다</b> — 이 저장소는 실행 이력을 판정 근거로 삼는다.
     */
    static final String ABANDON_CLAIM = """
            UPDATE BATCH_JOB_EXECUTION je
               SET je.VERSION = je.VERSION + 1
             WHERE je.JOB_EXECUTION_ID = :id
               AND je.STATUS IN ('STOPPING','STOPPED')
            """;


    /**
     * <b>선점문을 실행까지 감싼다.</b> 문자열만 내주면 부르는 쪽이 시각 파라미터를 원시
     * {@code LocalDateTime} 으로 바인딩할 수 있는데, 그 값은 드라이버의 존 정규화를
     * <b>안 탄다</b> — 컬럼은 스프링 배치가 {@code Timestamp.valueOf} 로 써서 세션 존(UTC)
     * 으로 렌더링한 값이라 두 축이 JVM 기본 존의 오프셋만큼 어긋난다.
     *
     * <p><b>실측은 {@link com.kafkick.batch.config.DefaultZoneGuard} 가 정본으로 든다.</b>
     * 요지만 옮기면 <b>부호를 따라 갈린다</b> — 조건이 {@code 메타시각 <= :stuckBefore} 라서다.
     * <b>UTC 동쪽</b>(KST 등)이면 컷오프가 오프셋만큼 앞서 조건이 <b>항상 참</b>이 되고,
     * 방금 뜬 실행이 시체 판정을 통과해 복구·중단 API 가 <b>도는 잡을 닫는다.</b>
     * <b>UTC 서쪽</b>이면 반대로 창이 넓어져 <b>진짜 시체도 오프셋만큼 늦게</b> 걷힌다 —
     * 덜 위험할 뿐 둘 다 틀린 답이다.
     *
     * <p><b>바인딩을 여기 한 곳에 가둔 이유가 그것이다.</b> 콜사이트마다 변환을 부르게 두면
     * 하나를 빠뜨려도 테스트가 안 잡는다 — 자바 쪽 {@code requireStuck} 이 먼저 막아
     * 살아 있는 실행은 이 문장까지 오지도 않고, 진짜 시체는 어느 축이든 1행이라
     * <b>성공 경로도 안 갈린다.</b>
     *
     * <p><b>가두는 것만으로는 부족해서 검사로 든다.</b> SQL 상수가 패키지 공개라 같은 패키지의
     * 서비스가 {@code jdbcClient.sql(CLAIM)} 으로 이 메서드를 <b>우회</b>할 수 있다 —
     * {@code StuckBeforeBindingIsCentralizedTest} 가 소스를 훑어 {@code stuckBefore} 바인딩이
     * 이 파일 하나뿐인지, 그리고 그 한 번이 {@code Timestamp.valueOf} 를 지나는지 센다.
     *
     * @param sql {@link #CLAIM} 또는 {@code VerifyStopService.CLAIM}. 검증은
     *            {@code STOPPING} 을 뺀 자기 문장을 쓴다
     */
    static int claim(JdbcClient jdbcClient, String sql, long executionId,
            LocalDateTime stuckBefore) {
        return jdbcClient.sql(sql)
                .param("id", executionId)
                .param("stuckBefore", Timestamp.valueOf(stuckBefore))
                .update();
    }

    /** 선점에 진 뒤 <b>현재 읽기</b>로 상태를 본다. 스냅샷 읽기는 옛 값을 준다(RR). */
    static final String CURRENT_STATUS = """
            SELECT STATUS FROM BATCH_JOB_EXECUTION
             WHERE JOB_EXECUTION_ID = :id FOR SHARE
            """;
}
