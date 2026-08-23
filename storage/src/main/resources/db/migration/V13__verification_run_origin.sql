-- 시드가 심은 기준 행과 배치가 만든 실행을 가릅니다.
--
-- 시드는 `verification_runs` 에 **완결된 과거 run** 을 일부러 심는다 — CLEAN 은 PASS 세 건,
-- CORRUPT 는 FAIL 과 정답 800행을 붙인 한 건이다(cy-seed/seedgen/stats.py). 게이트가 쓰는
-- as_of 도 그 행에서 나오므로 없앨 수 없다.
--
-- 그런데 판정 지표(cy_verification_verdict)가 "가장 최근에 닫힌 실행" 을 되읽으면 그 행을
-- 배치 판정으로 읽는다. 결과가 정확히 뒤집힌다:
--   CLEAN   검증을 한 번도 안 돌려도 PASS 가 나가 "안 돌린 채 통과했다" 가 성립한다
--   CORRUPT FAIL/800 이 상시 발화해 알림이 영원히 안 꺼진다
--
-- `rejectExistingRun` 이 같은 함정을 이미 막고 있었다 — *"이어받으면 검증기가 한 건도
-- 못 잡아도 정답이 나온다"*. 그 방어가 되읽기 경로에는 없어서 컬럼으로 세운다.
--
-- DEFAULT 'BATCH' 라 배치 INSERT 는 안 바뀐다. 시드만 명시적으로 'SEED' 를 쓴다 —
-- 뜻을 스키마에 남기면 대시보드와 게이트도 같은 구분을 쓸 수 있다.
-- ⚠️ 기존 행을 백필하지 않는다. MySQL 이 NOT NULL DEFAULT 로 전부 'BATCH' 로 채우는데,
--    이 마이그레이션이 도는 DB 에는 시드 행이 없으므로 그게 맞다 — 시드 데이터셋은 Flyway 가
--    아니라 cy-seed 의 ddl/00_schema.sql 로 만들어지고, 거기에는 이 컬럼이 처음부터 있다.
--
--    바꿔 말하면 **이 마이그레이션은 시드 데이터셋을 고치지 못한다.** cy-seed 1f217b5 이전에
--    만든 데이터셋에는 컬럼 자체가 없고 Flyway 가 그 DB 에 닿지 않는다. 그 DB 에 배치를 붙이면
--    findLatestClosed 의 `AND origin = 'BATCH'` 가 매번 Unknown column 으로 실패한다 —
--    되읽기가 직전 값을 유지하므로 조용하고, VerificationMetricsStale 이 그때 유일한 신호다.
--    **재생성이 유일한 답이다.** docs/14 시연 절차의 as_of 조회 블록에 그 판별 쿼리를 적어 뒀다.
ALTER TABLE verification_runs
  ADD COLUMN origin varchar(6) NOT NULL DEFAULT 'BATCH'
  COMMENT 'SEED / BATCH — 시드가 심은 기준 행인가, 배치가 만든 실행인가';

-- 불변식을 DB 제약으로 표현한다. varchar(6) 은 무엇이든 받는데, 되읽기가
-- WHERE origin = 'BATCH' 로 좁히므로 두 값 밖의 값은 그 실행을 지표에서
-- 조용히 지운다 — "판정이 없다"(NaN) 와 구분이 안 된다.
--
-- 시드 쪽에서 위치가 밀리는 경로가 실재한다. 로더가 컬럼 목록 없이 LOAD DATA 로
-- 넣기 때문에 15번째 자리가 한 칸 밀리면 엉뚱한 값이 여기 앉는다.
-- cy-seed 의 ddl/10_constraints_common.sql 에 같은 제약이 있다.
ALTER TABLE verification_runs
  ADD CONSTRAINT ck_verification_run_origin
  CHECK (origin IN ('SEED', 'BATCH'));
