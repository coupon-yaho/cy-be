-- 대시보드가 읽을 "완결된 최신 통계 스냅샷" 을 가리킵니다.
--
-- 통계 3종(coupon_stats · grade_stats · hourly_stats)은 run_id 를 PK 앞에 붙여 스냅샷으로
-- 쌓는다. 검증이 도중에 죽으면 세 테이블이 절반만 갱신된 채 남는데, 모든 통계 질의가 이 뷰를
-- 거치면 **완결되지 않은 스냅샷은 물리적으로 조회되지 않는다.** 화면이 늘어날 때 필터를
-- 빠뜨릴 수 없게 하려는 것이 요점이다.
--
-- CORRUPT 는 stats_status = SKIPPED 라 애초에 후보가 아니다. 오염 데이터 위의 집계는 뜻이 없다.
--
-- ⚠️ PRD-v4.15.md:953 의 정의에 `ORDER BY as_of DESC LIMIT 1` 로 적혀 있는데
--    **그것만으로는 비결정적이다.** 시드가 CLEAN 을 같은 as_of 에 attempt 1·2 로 심고
--    (결정론 증명용) 둘 다 stats_status = COMPLETE 다 — 즉 뷰가 만드는 상황이 아니라
--    시드가 실제로 만드는 상황에서 둘 중 아무 행이나 고른다.
--    id 를 두 번째 정렬 키로 넣어 "같은 시각이면 나중 시도" 로 못 박는다.
--    두 스냅샷이 같아야 정상이지만, 같은지를 검증하는 것과 뷰가 정하는 것은 다른 일이다.

CREATE VIEW `v_latest_stats_run` AS
SELECT `id`
  FROM `verification_runs`
 WHERE `dataset` = 'CLEAN'
   AND `stats_status` = 'COMPLETE'
 ORDER BY `as_of` DESC, `id` DESC
 LIMIT 1;
