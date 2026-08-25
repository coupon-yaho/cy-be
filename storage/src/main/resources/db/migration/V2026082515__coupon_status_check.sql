-- 회차 상태의 값 집합을 DB 가 강제하게 한다.
--
-- CY-446 이 이 컬럼에 값을 쓰는 **첫 코드**를 붙였다. 그런데 합법성을 지키는 것이
-- CouponRoundJdbcAdapter 의 SQL 리터럴뿐이었다 — 타입 시스템도 DB 도 안 받쳤다.
--
-- ⚠️ **조용히 틀리는 경로가 실재한다.** 회차 생성이 'SCHEDULED' 를 'PENDING' 으로 적어 넣으면
--    INSERT 는 통과하고, 전이 스케줄러는 그 회차를 영원히 안 열고,
--    cy_coupon_round_pending_open 도 안 센다(술어가 status='SCHEDULED' 다).
--    **게이지도 0 이고 알림도 없고 회차는 영원히 안 열린다** — CY-446 이 세운 관측 축이
--    통째로 눈이 먼다.
--
-- V10 이 issuances.status 에 같은 이유로 같은 제약을 걸었다("값 집합 밖이면 집계가 조용히
-- 틀린다"). coupons.status 에는 그 방어가 V1~V15 어디에도 없었다.
--
-- **cy-seed 에도 같은 제약을 넣었다** — `ddl/10_constraints_common.sql` @ a3eaa6d.
-- 검증용 셋(coupon_clean·coupon_corrupt)은 Flyway 가 안 닿으므로 그쪽이 없으면
-- **테스트만 초록이고 실물에는 제약이 없는** 상태가 된다 — SchemaParityTest 가 막으려는
-- 그 모양이다. 사본은 손으로 고치지 않고 원본에서 다시 받았다(seed-ddl/README.md 절차).
ALTER TABLE `coupons`
    ADD CONSTRAINT `ck_coupon_status`
        CHECK (`status` IN ('SCHEDULED', 'OPEN', 'CLOSED'));
