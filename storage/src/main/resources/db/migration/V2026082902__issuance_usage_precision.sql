-- ⚠️ **번호가 V2026082502 에서 밀렸다(CY-744).** main 이 같은 날 같은 번호를 다른 파일에
--    쓰고 있어서다 — 날짜 기반 번호를 두 브랜치가 독립으로 붙이면 이렇게 겹친다.
--    Flyway 는 그 상태를 "Found more than one migration with version V2026082502" 로
--    거절하고 앱이 아예 안 뜬다(실측). 아직 어느 배포에도 적용 안 된 파일이라 이름을 바꿨다.
--
-- 사용 행의 시각 정밀도를 이력과 맞춥니다.
--
-- issuance_histories.created_at 은 datetime(6) 인데 issuance_usages 의 두 시각은 datetime 이었습니다.
-- 같은 사건을 기록하는 두 축의 정밀도가 다르면 MySQL 이 소수 초를 **반올림**해 저장하므로,
-- 이력은 13:00:00.700000 인데 사용 행은 13:00:01 이 됩니다.
--
-- V5 는 활성 사용을 `used_at <= asOf AND (canceled_at IS NULL OR canceled_at > asOf)` 로 판정하고
-- asOf 는 datetime(6) 입니다. asOf 가 그 반올림 간격 안에 놓이면
-- "접힌 상태는 USED 인데 활성 사용 0" 이 되어 **오염이 없는 데이터에서 V5 가 웁니다.**
-- 정상셋 0건이 검증 배치의 합격 조건이라 이 오탐 하나가 게이트를 떨어뜨립니다.
--
-- 정밀도를 넓히는 변경이라 기존 값과 기존 코드에 영향이 없습니다.
--
-- ⚠️ 이 마이그레이션만으로는 **이미 반올림돼 저장된 값이 복구되지 않습니다.**
--    13:00:00.700000 로 들어갔다가 13:00:01 로 굳은 행은 13:00:01.000000 이 될 뿐입니다.
--    적용 뒤에는 issuance_usages 를 재시드해야 오탐이 실제로 사라집니다.
--
-- ⚠️ 게이트가 도는 CLEAN/CORRUPT 스키마는 시드 저장소가 자기 DDL 로 만듭니다.
--    거기에는 이 마이그레이션이 닿지 않으므로 시드 저장소의 DDL 도 함께 고쳐야 합니다
--    (coupon-yaho/cy-seed-data-generator).
--
-- ⚠️ 시간형 정밀도 변경은 테이블 재구축이라 그동안 issuance_usages 에 대한 DML 이 막힙니다.
--    데이터가 이미 있는 DB 에서는 적재 전에 돌리십시오.

ALTER TABLE `issuance_usages`
    MODIFY `used_at`     datetime(6) NOT NULL COMMENT '사용 시각. 이력과 같은 정밀도',
    MODIFY `canceled_at` datetime(6) NULL COMMENT '사용 취소 시각. NULL 이면 활성';
