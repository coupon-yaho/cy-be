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
--
-- FK 의존 없음을 확인했다: `issuances.code` · `members.email_hash` 를 참조하는 FK 가 없어
-- (V1 의 REFERENCES 절 전수 확인) 인덱스를 떼도 MySQL 이 막지 않는다.

ALTER TABLE `issuances` DROP INDEX `code`;
CREATE UNIQUE INDEX `uk_coupon_code` ON `issuances` (`code`);

ALTER TABLE `members` DROP INDEX `email_hash`;
CREATE UNIQUE INDEX `uk_email_hash` ON `members` (`email_hash`);
