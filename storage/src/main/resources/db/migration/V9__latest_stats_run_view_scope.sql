-- 최신 통계 뷰의 정렬 계약을 SQL 로 표현하고 범위를 좁힙니다.
--
-- V8 은 "같은 시각이면 나중 시도" 를 계약으로 적어 놓고 `id DESC` 로 구현했다.
-- id 는 AUTO_INCREMENT(삽입 순서)이고 attempt 는 잡 파라미터다 — 둘의 순서를 맞추는
-- 제약이 없다. 결정론 증명을 attempt 3 → 4 → 2 순서로 돌리면 id 는 4·5·6 이라
-- 계약이 말한 "나중 시도"(4)가 아니라 attempt 2 를 고른다.
--
-- scope 필터도 없었다. 증분 검증은 (from_ts, as_of] 윈도우 집계라 대시보드의 분모가
-- 될 수 없는데, rejectUnsupportedScope 가 풀리는 날 stats_status = COMPLETE 로 들어와
-- 뷰의 답이 된다. finalizeRunStep 은 그때를 대비해 이미 decidesVerdict 가드를 심어 뒀다 —
-- 뷰에는 대응하는 장치가 없었다.
--
-- id DESC 는 최종 타이브레이커로 남긴다. uk_run_params(as_of, dataset, scope, attempt) 가
-- 이미 유일성을 보장해 도달하지 않지만, 조건이 하나 늘 때 유일성이 깨질 수 있고
-- **뷰는 결정적이어야 한다.**

CREATE OR REPLACE VIEW `v_latest_stats_run` AS
SELECT `id`
  FROM `verification_runs`
 WHERE `dataset` = 'CLEAN'
   AND `scope` = 'FULL'
   AND `stats_status` = 'COMPLETE'
 ORDER BY `as_of` DESC, `attempt` DESC, `id` DESC
 LIMIT 1;
