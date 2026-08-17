-- 오염셋 판정이 어느 정답 묶음과 대조했는지를 실행 행에 남깁니다.
--
-- CY-196 이 양방향 대조를 붙였을 때 seedRunId 는 **잡 실행 컨텍스트에만** 있었다.
-- 그래서 PASS 로 닫힌 행 하나만 보고 "정답 800행과 정확히 일치했다" 를 주장해야 하는데
-- 그 행에는 대조 상대가 없었다. 주입을 두 번 돌려 묶음이 둘인 DB 에서 정확히 그 질문이 오고,
-- 잡 메타를 지우는 정리 배치가 한 번 돌면 컨텍스트도 사라져 영영 답할 수 없다.
--
-- CLEAN 은 NULL 이다. 정상셋은 대조할 묶음이 없다 — 검출 0건이 곧 통과다.
--
-- FK 를 걸지 않는다. expected_findings.seed_run_id 는 묶음 하나에 800행이 붙는 값이라
-- 유니크가 아니고, verification_runs.id 와도 별도 네임스페이스다
-- (cy-seed/ddl/00_schema.sql 의 그 컬럼 주석).

ALTER TABLE `verification_runs`
    ADD COLUMN `seed_run_id` bigint NULL
        COMMENT '대조한 정답 묶음. CORRUPT 만 채운다 — CLEAN 은 대조 상대가 없다'
        AFTER `dataset`;
