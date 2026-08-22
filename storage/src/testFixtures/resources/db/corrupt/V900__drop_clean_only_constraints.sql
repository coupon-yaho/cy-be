-- CORRUPT 스키마 모양을 테스트에서 재현합니다. 운영 산출물이 아닙니다.
--
-- ⚠️ 원본은 시드 저장소입니다 — cy-seed-data-generator 의
--    ddl/11_constraints_clean.sql (거는 쪽) 과 ddl/12_constraints_corrupt.sql (안 거는 쪽).
--    실제 coupon_corrupt 스키마는 그쪽이 만들고, 여기는 그 모양을 흉내 낼 뿐입니다.
--    그래서 main 이 아니라 testFixtures 에 둡니다 — jar 에 실리면 cy-be 가 CORRUPT
--    스키마의 두 번째 주인처럼 보이고, 둘이 어긋나도 아무도 모르게 됩니다.
--
-- ⚠️ 여기서 떨어뜨리는 셋이 곧 "오염이 물리적으로 가능한 이유" 입니다.
--    하나라도 남으면 주입이 INSERT 단계에서 튕겨 규칙이 검출할 대상 자체가 안 생기고,
--    테스트는 "검출 0건" 을 정상으로 읽습니다 — 규칙이 틀려도 초록입니다.
--
--    uk_coupon_member   오염 유형 6 — 동일 회원이 같은 회차에서 2건
--    uk_coupon_code     오염 유형 5 — 같은 code 를 다른 회원에게 복제
--    ck_stock_range     오염 유형 1(+1) · 3(-1) — 재고를 범위 밖으로 민다
--
--    V1 은 `code char(16) UNIQUE` 로 선언해 이름을 안 줬고 MySQL 이 컬럼명을 그대로 썼습니다.
--    V6__name_unique_constraints.sql 이 uk_coupon_code 로 개명했습니다 — 여기서 떨어뜨리는
--    이름은 그 이후 이름입니다.

-- ⚠️ 대체 인덱스를 먼저 깝니다. uk_coupon_member 는 (coupon_id, member_id) 라
--    coupon_id FK 가 쓰는 유일한 인덱스이기도 해서, 그냥 떨어뜨리면 MySQL 이
--    "Cannot drop index 'uk_coupon_member': needed in a foreign key constraint" 로 막습니다.
--
--    시드 저장소의 CORRUPT 스키마에는 uk_coupon_member 가 애초에 없고, FK 를 걸 때
--    MySQL 이 coupon_id 인덱스를 자동으로 만듭니다. 여기서는 이미 있는 것을 떼는 순서라
--    그 자동 생성분을 손으로 만들어 주는 셈입니다 — 최종 모양은 같습니다.
-- 이름이 `coupon_id` 인 것은 우연이 아니다. 시드 CORRUPT 에는 uk_coupon_member 가 애초에
-- 없어, FK 를 걸 때 MySQL 이 자식 컬럼에 인덱스를 자동 생성하고 그 이름이 `coupon_id` 다.
-- 여기서 다른 이름을 주면 두 스키마의 최종 모양이 갈린다 — CorruptSchemaParityTest 가 잡는다.
CREATE INDEX `coupon_id` ON `issuances` (`coupon_id`);

DROP INDEX `uk_coupon_member` ON `issuances`;

DROP INDEX `uk_coupon_code` ON `issuances`;

ALTER TABLE `coupon_stocks` DROP CHECK `ck_stock_range`;

-- 시드 CORRUPT 가 expected_findings 에 거는 보조 인덱스(ddl/12_constraints_corrupt.sql).
-- 떼는 것만 재현하고 더하는 것을 빼면 두 스키마의 실행계획이 달라져,
-- 판정 리포트의 성능 결론이 실제 오염셋으로 옮겨지지 않는다.
CREATE INDEX `idx_expected_type` ON `expected_findings` (`corrupt_type`);
