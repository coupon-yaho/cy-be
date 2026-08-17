// 대시보드가 읽을 통계 스냅샷을 만듭니다.
package com.kafkick.core.verification;

import java.time.LocalDateTime;
import java.util.List;

/**
 * <b>스냅샷이다.</b> 세 테이블 모두 {@code run_id} 를 PK 앞에 두고 쌓는다 — 검증이 도중에 죽으면
 * 절반만 갱신된 채 남는데, {@code v_latest_stats_run} 이 {@code stats_status = 'COMPLETE'} 인
 * 실행만 가리키므로 <b>완결되지 않은 스냅샷은 물리적으로 조회되지 않는다.</b>
 *
 * <p><b>합격한({@code verdict = PASS}) CLEAN 전용이다.</b> 오염 데이터 위의 집계는 뜻이 없는데,
 * CLEAN 에서 검출이 났다는 것도 <b>그 데이터가 실제로 어긋났다</b>는 뜻이라 같은 근거가 적용된다.
 * 둘 다 {@code stats_status = 'SKIPPED'} 로 남는다 — 같은 값이 두 뜻을 가지므로
 * 구분은 같은 행의 {@code dataset}·{@code verdict} 로 한다.
 *
 * <p><b>값의 정본은 시드 구현이다.</b> {@code contract.json} 에 통계 조항이 없어 checksum·지문과
 * 달리 계약으로 고정돼 있지 않다. 그래서 필드마다 시드가 <b>실제로 읽는 컬럼</b>을 그대로 쓴다 —
 * 두 값이 지금 같더라도 같다는 사실에 기대지 않는다:
 *
 * <pre>
 * hourly           ISSUE 이력의 created_at   (시드는 _history() 안에서 센다)
 * sold_out_seconds issuances.issued_at 의 MAX (시드는 루프 변수 last_issue_at 을 쓴다)
 * issued/used/…    issuances.status           (리플레이 상태가 아니다)
 * 완판 판정         issued_total >= coupon_stocks.total_quantity
 * </pre>
 *
 * <p><b>{@code active_count} 로 완판을 판정하면 안 된다.</b> 그것은 <i>현재 보유량</i>
 * (ISSUED + USED)이라 취소·만료로 줄어든다 — 미달 회차도 0 이 될 수 있고 완판 회차도 0 이 아닐 수
 * 있다. 시드가 완판을 정하는 식은 {@code issue_count >= total_quantity} 다.
 */
public interface StatsRepository {

    /**
     * 같은 실행의 앞 스냅샷을 지운다.
     *
     * <p><b>지금은 지울 행이 없다.</b> {@code preventRestart()} 와 {@code rejectExistingRun} 이
     * 같은 {@code runId} 의 두 번째 집계를 막는다 — 같은 파일이 <i>"재사용이 막으려던 것은
     * {@code preventRestart()} 때문에 도달할 수 없다"</i> 고 이미 적어 놨다. 그 둘 중 하나가
     * 풀리는 날 {@code (run_id, coupon_id)} 중복키로 죽는 것을 막는 자리이므로 남긴다.
     */
    void clear(long runId);

    /**
     * 회차 수. {@link #aggregateCouponStats} 가 쓴 행 수와 같아야 한다 —
     * 그 등식이 "발급 0건 회차도 행을 받는다" 를 런타임에 지키는 유일한 수단이다.
     */
    int couponCount();

    /**
     * <b>{@code ISSUE} 이력이 없는 발급건.</b> CLEAN 에서는 발급건마다 그 이력이 정확히 하나이므로
     * 0 이어야 한다 — 0 이 아니면 데이터가 구조적으로 깨진 것이다.
     *
     * <p><b>총합 비교로 쓰면 안 된다.</b> 한때
     * {@code COUNT(issuances) == SUM(hourly)} 로 뒀는데 <b>대칭 오차를 못 잡는다</b> —
     * 이력 없는 발급건 하나와 이력이 둘인 발급건 하나가 있으면 총합이 같아 그냥 통과한다.
     * 이 저장소가 {@code finding_count} 비교를 거부한 것과 정확히 같은 형태다
     * (<i>"오탐 400 + 누락 400 도 800"</i>).
     *
     * <p>이 질의가 덮는 사각은 CY-196 이 기록해 둔 것이다 — 이력이 없는 발급건은
     * {@code asof_state} 에 안 실려 V3·V5 의 시야 밖이고, V4 는 반대 방향(고아 이력)만 본다.
     *
     * <p>창은 리플레이와 같다 — {@code id <= maxHistoryId AND created_at <= asOf}.
     */
    int countIssuancesWithoutIssueHistory(LocalDateTime asOf, long frozenMaxHistoryId);

    /** 위 질의가 0 이 아닐 때 메시지에 실을 발급건 id 표본. */
    List<Long> sampleIssuancesWithoutIssueHistory(
            LocalDateTime asOf, long frozenMaxHistoryId, int limit);

    /**
     * 회차별 집계. <b>발급이 0건인 회차도 행을 쓴다</b> — 없는 행과 0인 행은 다르고,
     * 대시보드가 "데이터 없음" 과 "0건" 을 구분해야 한다. 시드도 카탈로그 전체를 돈다.
     *
     * @return 쓴 행 수. 회차 수와 같아야 한다
     */
    int aggregateCouponStats(long runId, LocalDateTime asOf);

    /**
     * {@code (회차, 발급 시점 등급)} 별 집계. <b>실제로 존재하는 쌍만</b> 쓴다 — 시드가
     * {@code totals.grade} 에 누적된 키만 쓰므로 없는 조합에 0 행을 만들면 어긋난다.
     *
     * <p>등급은 {@code issuances.issued_grade} 스냅샷이다. {@code members} 를 읽으면 그 뒤
     * 등급이 바뀐 회원의 과거 발급이 잘못 분류된다.
     */
    int aggregateGradeStats(long runId, LocalDateTime asOf);

    /**
     * 요일·시각별 발급 수. <b>리플레이와 같은 창을 쓴다</b> —
     * {@code id <= frozenMaxHistoryId AND created_at <= asOf}.
     *
     * <p>{@code created_at <= asOf} 만 걸면 <b>다시 재는 것</b>이라, 얼린 상한보다 큰 id 인데
     * 시각이 과거인 백데이트 이력을 리플레이는 못 읽고 통계는 읽는다. CY-193 에서
     * {@code dataset_fingerprint} 가 앓았던 병이 그것이고 {@code hasHistoriesAddedAbove} 가
     * 그래서 생겼다.
     *
     * <p>있는 칸만 돌려준다. 7 × 24 를 채우는 것은 호출자 몫이다.
     */
    List<HourlyIssued> issuedByHour(long frozenMaxHistoryId, LocalDateTime asOf);

    /**
     * 요일·시각 스냅샷을 쓴다. <b>168행 전부를 넘겨야 한다</b> — 빈 칸을 빼면 대시보드가
     * "그 시각에 데이터가 없다" 와 "그 시각에 0건이다" 를 구분할 수 없다.
     */
    void appendHourlyStats(long runId, List<HourlyIssued> hourly);
}
