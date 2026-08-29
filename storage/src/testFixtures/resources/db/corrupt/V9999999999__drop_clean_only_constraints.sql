-- CORRUPT 스키마 모양을 테스트에서 재현합니다. 운영 산출물이 아닙니다.
--
-- ⚠️ 번호가 V9999999999 인 것은 "항상 맨 마지막" 을 뜻합니다. 이 문장은 앞의 마이그레이션들이
--    만든 제약을 떨어뜨리므로 그것들보다 뒤에 와야 합니다. 한때 V900 이었는데, 연번이
--    날짜형(V2026082501…)으로 옮겨 가면서 900 < 2026082504 가 되어 **순서가 뒤집혔습니다** —
--    uk_coupon_code 가 생기기도 전에 떨어뜨리려다 1091 로 죽었습니다. 날짜형보다 큰 값이라야
--    그 뜻이 유지됩니다 — **자릿수를 세십시오.** 처음에 V99999999(8자리)로 고쳤다가 같은
--    자리에서 또 죽었습니다: 99,999,999 < 2,026,082,504 입니다.
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
--    V2026082505__name_unique_constraints.sql 이 uk_coupon_code 로 개명했습니다 — 여기서 떨어뜨리는
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
-- ⚠️ **더 안 만든다(CY-744).** main 의 V2026082502 가 idx_issuances_coupon_id
--    (coupon_id, id) 를 만들어서 uk_coupon_member 를 떼도 FK 가 쓸 인덱스가 남는다.
--    여기서 또 만들면 시드에 없는 인덱스가 생겨 파리티가 그 자리를 잡는다.

DROP INDEX `uk_coupon_member` ON `issuances`;

DROP INDEX `uk_coupon_code` ON `issuances`;

ALTER TABLE `coupon_stocks` DROP CHECK `ck_stock_range`;

-- ── CY-744 합류로 늘어난 것들 ────────────────────────────────────────────────
--
-- main 이 같은 불변식을 자기 이름으로 한 겹 더 걸어 뒀습니다. 하나라도 남으면 주입이
-- INSERT 단계에서 튕겨 **규칙이 검출할 대상 자체가 안 생기고**, 테스트는 "검출 0건" 을
-- 정상으로 읽습니다 — 규칙이 틀려도 초록입니다. 위 목록과 같은 이유로 전부 뗍니다.
--
--   ck_coupon_stock_active_range   재고 범위(V3) — ck_stock_range 와 같은 축, 이름만 다름
--
-- ⚠️ **다섯을 뺐다가 되돌렸다.** 아래는 CLEAN·CORRUPT 공통이다 — 오염 700건 중
--    이 제약을 넘어야 하는 주입이 **하나도 없다**(cy-seed/seedgen/corrupt.py 전수 확인):
--
--      ck_coupon_stock_total_positive   total_quantity 를 만지는 주입 없음
--      ck_coupon_round_time_range       open_at·close_at 을 만지는 주입 없음
--      uk_issuance_usages_active        활성 사용 2건을 심는 유형 없음
--                                       (유형 3 은 [(t1,t2),(t3,None)], 유형 7 은 [(t1,None)])
--      uk_issuance_usages_issuance_order order_id 는 사용마다 난수
--      ck_issuance_usages_cancel_time   canceled_at < used_at 을 심는 유형 없음
--
--    필요 없는데 떼면 **오염과 무관한 사고가 CORRUPT 에서만 조용히 통과한다** —
--    이 파일 머리말이 적은 원칙("여기서 떨어뜨리는 셋이 곧 오염이 물리적으로 가능한
--    이유")의 반대 방향 실패다.
ALTER TABLE `coupon_stocks` DROP CHECK `ck_coupon_stock_active_range`;

-- ⚠️ **uk_issuance_usages_active 도 떼지 않는다.** 한때 여기 "V5 가 이중 사용을
--    심어야 하니 떼야 한다" 고 적혀 있었는데 **위 표가 그 반대를 실측해 뒀다** —
--    활성 사용 2건을 심는 유형이 없다(유형 3 은 [(t1,t2),(t3,None)], 유형 7 은
--    [(t1,None)]). V5(USAGE_MISMATCH)가 잡는 것은 "활성 사용이 둘" 이 아니라
--    **issuances.status 와 활성 사용 유무가 어긋난 것**이다. 그 오염은 이 UNIQUE 를
--    건드리지 않는다.
-- ⚠️ **상태 어휘 CHECK 두 개도 떼지 않는다.** 한 번 떼려다 되돌렸다 —
--    "시드 CORRUPT 가 안 건다" 고 적었는데 **확인 안 하고 쓴 것이었다.**
--    실제로 시드는 그 둘을 10_constraints_common.sql(공통)에 두므로 CORRUPT 에도 있다.
--    오염 유형이 규약 밖 상태를 심지도 않아서 뗄 이유가 없다.

-- 시드 CORRUPT 가 expected_findings 에 거는 보조 인덱스(ddl/12_constraints_corrupt.sql).
-- 떼는 것만 재현하고 더하는 것을 빼면 두 스키마의 실행계획이 달라져,
-- 판정 리포트의 성능 결론이 실제 오염셋으로 옮겨지지 않는다.
CREATE INDEX `idx_expected_type` ON `expected_findings` (`corrupt_type`);
