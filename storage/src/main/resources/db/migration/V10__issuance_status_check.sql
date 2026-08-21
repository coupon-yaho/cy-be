-- 발급건 상태를 네 값으로 못 박습니다.
--
-- 통계가 issued_total 을 COUNT(*) 로, issued/used/cancelled/expired 를 SUM(status = 'X') 로
-- 센다. status 가 네 값 밖이면 그 행은 **분모에만 남고 어느 버킷에도 안 들어간다** —
-- 퍼널 등식이 조용히 깨지고, 대시보드에서는 데이터 문제가 아니라 "발급률이 낮다" 로 보인다.
--
-- 컬럼이 varchar(12) 라 어떤 문자열도 들어갔다. 운영 SQL 로 상태를 손보다 'CANCELED'(l 하나)
-- 같은 오타를 넣으면 규칙 여섯 중 아무도 그것을 이름으로 잡지 않는다.
--
-- 불변식을 애플리케이션 로직이 아니라 DB 제약으로 표현한다(설계 원칙 1번).
-- CLEAN 전용이 아니다 — 오염셋도 이 네 값만 쓴다(cy-seed 의 STATUSES).

-- 적용 전 점검 둘. Flyway 가 이 구문을 돌릴 때 **기존 행을 전부 검사**하므로,
-- 네 값 밖이 하나라도 있으면 마이그레이션이 error 3819 로 실패하고 그 자리에서 멈춘다.
--
--   SELECT status, COUNT(*) FROM issuances GROUP BY status;   -- 네 값뿐인지
--   SELECT VERSION();                                          -- 8.0.16 이상인지
--
-- 8.0.16 미만은 CHECK 를 **파싱만 하고 무시한다** — 초록으로 통과하는데 아무것도 안 막는
-- 상태가 되어 가장 나쁘다. 8.0.35 에서 거부되는 것을 확인했다.
-- 다만 테스트 컨테이너는 mysql:latest 라 버전이 바뀔 수 있다 — 이 CHECK 가 조용히
-- 무시되는 서버로 넘어가면 파리티 테스트가 먼저 운다.

ALTER TABLE `issuances`
    ADD CONSTRAINT `ck_issuance_status`
        CHECK (`status` IN ('ISSUED', 'USED', 'CANCELLED', 'EXPIRED'));
