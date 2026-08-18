// 통계 집계 Step 의 결과입니다. 대시보드가 읽습니다.
package com.kafkick.core.verification;

/**
 * SKIPPED 는 두 뜻을 갖습니다. 구분은 같은 행의 dataset·verdict 로 합니다:
 *
 * <pre>
 * dataset=CORRUPT                  오염 데이터 위의 집계는 의미가 없다        정상
 * dataset=CLEAN 이고 verdict!=PASS 정합성 불합격 데이터 위의 집계도 뜻이 없다  경보
 * </pre>
 *
 * 둘 다 대시보드는 직전 합격 CLEAN 스냅샷을 그대로 보여줍니다 — 뒤쪽은 그 사실이
 * 조용히 넘어가면 안 되는 상태입니다.
 *
 * <p>PARTIAL 은 아직 아무도 쓰지 않습니다. 통계 Step 이 죽으면 트랜잭션이 롤백되어
 * 부분 스냅샷이 물리적으로 생기지 않으므로 표현할 상태가 없습니다.
 *
 * 제출 직전 마지막 실행이 CORRUPT 면 v_latest_stats_run 뷰가 이전 run 을 가리켜
 * 통계 차트가 조용히 옛 데이터를 보여줍니다.
 */
public enum StatsStatus {

    COMPLETE,
    PARTIAL,
    SKIPPED
}
