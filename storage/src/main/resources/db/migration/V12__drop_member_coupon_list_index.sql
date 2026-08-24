-- 회원 쿠폰 목록용 성능 인덱스를 제거하되 외래키용 최소 인덱스는 유지합니다.
ALTER TABLE `issuances`
    ADD INDEX `fk_issuances_member` (`member_id`);

DROP INDEX `idx_issuances_member_issued` ON `issuances`;
