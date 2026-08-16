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

ALTER TABLE `issuance_usages`
    MODIFY `used_at`     datetime(6) NOT NULL COMMENT '사용 시각. 이력과 같은 정밀도',
    MODIFY `canceled_at` datetime(6) NULL COMMENT '사용 취소 시각. NULL 이면 활성';
