-- `PASS 0건` 의 분모를 남깁니다.
--
-- "검출 0건" 은 그 자체로 아무것도 증명하지 않는다. **다 보고 못 찾은 것**과
-- **거의 아무것도 안 본 것**이 리포트에서 같은 모양이기 때문이다. 지금 그 둘을 가를
-- 재료가 어디에도 없다:
--
--   dataset_fingerprint  재료에 issuance_count·history_count 가 들어 있지만 SHA-256 한
--                        덩어리다 — 같음/다름만 말하고 얼마인지는 말하지 않는다
--   stepReadTotal        모든 Step 의 READ_COUNT 를 그냥 더한 값이고 세는 단위가 Step 마다
--                        다르다. BatchRunView 가 "처리 건수로 칠하지 마라" 고 적어 뒀다
--
-- 사전예약 PRD 의 대사 알고리즘 5단계가 요구하는 축이다 —
-- *"검사 건수, 정정 건수, 복구 건수, 잔여 불일치 건수를 보고서로 기록한다."*
-- 정정·복구는 이 저장소의 검증이 쓰기를 안 하니 범위 밖이고, **검사 건수**는 지금도
-- 남겨야 하는 값이다.
--
-- ⚠️ **판정에는 안 쓴다.** 처음에 "검사 대상이 0이면 판정 불가" 가드를 넣으려다 철회했다 —
--    재고 불일치 규칙이 `coupons` 에서 시작해 발급건을 LEFT JOIN 하므로 **발급건 0에서도
--    검출을 낸다.** 재고가 5인데 발급이 0인 상태가 바로 그 규칙이 잡아야 하는 사고라,
--    거기에 가드를 걸면 진짜 검출을 덮는다(기존 시험 셋이 실제로 깨졌다).
--    이 수들은 판정이 아니라 **읽는 사람이 판단할 재료**다.
--
-- ⚠️ **자리가 계약이다.** SchemaParityTest 가 information_schema 의 ordinal_position 까지
--    비교하므로 시드 DDL 과 같은 자리여야 한다 — cy-seed @ 7415dfe 가 dataset_fingerprint
--    바로 뒤에 두었고 여기도 그렇게 둔다.
--
-- NULL 을 허용한다. 이 마이그레이션 이전 실행에는 그 값이 없고, 없는 것을 0 으로 채우면
-- **"안 봤다" 로 읽힌다** — 이 컬럼이 막으려는 바로 그 오독이다.
ALTER TABLE `verification_runs`
    ADD COLUMN `examined_issuance_count` bigint NULL
        COMMENT 'as_of 시점의 발급건 수 — PASS 0건의 분모다'
        AFTER `dataset_fingerprint`,
    ADD COLUMN `examined_history_count` bigint NULL
        COMMENT 'as_of 까지의 이력 행 수. 리플레이가 보는 범위'
        AFTER `examined_issuance_count`;
