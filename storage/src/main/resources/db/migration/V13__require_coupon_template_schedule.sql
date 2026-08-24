-- 쿠폰 템플릿 일정 필드의 도메인 필수 조건을 DB 제약과 일치시킵니다.
ALTER TABLE `coupon_templates`
    MODIFY COLUMN `nth_week` tinyint NOT NULL,
    MODIFY COLUMN `day_of_week` varchar(3) NOT NULL,
    MODIFY COLUMN `start_time` time NOT NULL,
    MODIFY COLUMN `duration_hours` int NOT NULL;
