-- CLEAN 스키마 전용 물리 제약.
--
-- 이 세 개가 CLEAN 에만 있다는 사실 자체가 "불변식은 애플리케이션이 아니라
-- DB 제약으로 표현한다"(PRD 설계 원칙 1)의 증거다. 오염셋은 정의상 이걸 위반해야
-- 하므로 CORRUPT 스키마에서는 걸지 않는다.

-- 1인 1매 — 취소·만료 후에도 재발급 불가. 오염 유형 6 이 위반한다.
CREATE UNIQUE INDEX uk_coupon_member ON issuances (coupon_id, member_id);

-- 발급 코드 유일. 오염 유형 5 가 위반한다.
CREATE UNIQUE INDEX uk_coupon_code ON issuances (code);

-- 초과 발급 방어. 오염 유형 1·3 이 재고를 흔든다.
ALTER TABLE coupon_stocks
  ADD CONSTRAINT ck_stock_range
  CHECK (active_count >= 0 AND active_count <= total_quantity);

-- ── CY-744 합류로 늘어난 CLEAN 전용 제약 ────────────────────────────────────
-- CORRUPT 는 이것들을 안 건다(12_constraints_corrupt.sql). cy-be 쪽에서는
-- V9999999999__drop_clean_only_constraints.sql 이 같은 목록을 뗀다 —
-- **두 목록이 갈리면 오염 주입이 한쪽에서만 튕겨 검출 0건이 정상으로 읽힌다.**

-- 사용 이력 — V8__add_issuance_usage_uniqueness.sql
-- uk_issuance_usages_active 가 "발급건 하나에 활성 사용은 하나" 를 DB 로 막는다.
-- 그것이 정확히 V5(DOUBLE_USE)가 검출하는 오염이라 CORRUPT 에는 없어야 한다.
ALTER TABLE issuance_usages
    ADD CONSTRAINT uk_issuance_usages_issuance_order UNIQUE (issuance_id, order_id),
    ADD CONSTRAINT uk_issuance_usages_active UNIQUE (active_issuance_id);

-- 취소 시각 역전 방지 — V9__add_issuance_usage_cancel_time_constraint.sql
ALTER TABLE issuance_usages
    ADD CONSTRAINT ck_issuance_usages_cancel_time
        CHECK (canceled_at IS NULL OR canceled_at >= used_at);

-- 재고·회차 창 — V3__add_coupon_round_stock_constraints.sql
-- ck_stock_range 와 같은 축을 main 이 자기 이름으로 한 겹 더 건 것이다.
ALTER TABLE coupon_stocks
    ADD CONSTRAINT ck_coupon_stock_total_positive CHECK (total_quantity > 0),
    ADD CONSTRAINT ck_coupon_stock_active_range
        CHECK (active_count >= 0 AND active_count <= total_quantity);

ALTER TABLE coupons
    ADD CONSTRAINT ck_coupon_round_time_range CHECK (close_at > open_at);
