-- 재고 불변식을 DB 제약으로 세웁니다. CLEAN 스키마 전용입니다.
--
-- V1__init_schema.sql:289 가 이 제약을 문서로 적어 두었는데 실제 DDL 은 빠져 있었습니다.
-- PRD 설계 원칙 1번이 "불변식은 애플리케이션이 아니라 DB 제약으로 표현한다 —
-- 로직에 버그가 있어도 막혀야 한다" 이므로, 적어만 두고 안 건 것은 원칙을 어긴 것입니다.
--
-- ⚠️ 이 제약은 CORRUPT 스키마에 걸지 않습니다. 오염 유형 1 이 재고를 +1 하고 유형 3 이
--    -1 하므로 범위를 벗어나는 값이 의도적으로 들어갑니다. 시드 저장소의
--    ddl/11_constraints_clean.sql 과 ddl/12_constraints_corrupt.sql 이 같은 방식으로 갈라 둡니다.
--    cy-be 테스트에서 그 모양을 재현하는 것은
--    storage/src/testFixtures/resources/db/corrupt/V9999999999__drop_clean_only_constraints.sql 입니다.
--
-- ⚠️ active_count 는 누적 발급 수가 아니라 현재 보유량(ISSUED + USED)입니다.
--    누적으로 읽으면 취소가 쌓일 때 이 제약이 거짓으로 터집니다.

ALTER TABLE `coupon_stocks`
    ADD CONSTRAINT `ck_stock_range`
        CHECK (`active_count` >= 0 AND `active_count` <= `total_quantity`);
