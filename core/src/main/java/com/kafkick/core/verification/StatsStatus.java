// 통계 집계 Step 의 결과입니다. 대시보드가 읽습니다.
package com.kafkick.core.verification;

/**
 * CORRUPT run 은 통계 Step 을 아예 실행하지 않고 SKIPPED 로 남깁니다.
 * 오염 데이터 위의 집계는 의미가 없고, 대시보드는 직전 CLEAN 스냅샷을 그대로 보여줍니다.
 *
 * 제출 직전 마지막 실행이 CORRUPT 면 v_latest_stats_run 뷰가 이전 run 을 가리켜
 * 통계 차트가 조용히 옛 데이터를 보여줍니다.
 */
public enum StatsStatus {

    COMPLETE,
    PARTIAL,
    SKIPPED
}
