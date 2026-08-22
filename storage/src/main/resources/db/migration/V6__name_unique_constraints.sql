-- 인라인 UNIQUE 를 명명 인덱스로 바꿉니다. 이름이 곧 계약입니다.
--
-- V1 이 `code char(16) UNIQUE` 처럼 컬럼에 붙여 썼는데, 그러면 MySQL 이 제약 이름을
-- **컬럼명으로** 자동 생성한다. 그래서 이 저장소의 제약 이름은 `code` · `email_hash` 였다.
--
-- 문제는 그 이름이 다른 두 곳과 다르다는 것이다:
--   docs/PRD-v4.15.md:382,1667      uk_email_hash
--   cy-seed/ddl/11_constraints_clean.sql  uk_coupon_code · uk_email_hash (10_constraints_common)
--
-- 이름이 갈리면 **같은 스크립트를 두 스키마에 못 돌린다.** 실제로
-- V900__drop_clean_only_constraints.sql 이 `DROP INDEX code` 라고 적어야 했는데,
-- 시드가 만든 CORRUPT DB 에서는 그 이름이 없어 같은 문장이 실패한다.
--
-- V1 을 직접 고치지 않는다 — 이미 적용된 마이그레이션의 체크섬을 바꾸면 Flyway 가 거부한다.

-- RENAME INDEX 다. DROP 뒤 CREATE 로 쓰면 안 된다 — MySQL 의 DDL 은 트랜잭션이 아니라
-- 각 문장이 암묵 커밋이다. 두 문장 사이에는 유니크가 없는 창이 열리고, 그 틈에 중복이
-- 들어오면 CREATE 가 실패해 **인덱스만 사라진 상태**로 남는다. 이름을 바꾸는 일에
-- 제약을 잠시 걷어낼 이유가 없다.

ALTER TABLE `issuances` RENAME INDEX `code`       TO `uk_coupon_code`;
ALTER TABLE `members`   RENAME INDEX `email_hash` TO `uk_email_hash`;
