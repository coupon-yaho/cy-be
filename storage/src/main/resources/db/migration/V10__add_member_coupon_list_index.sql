-- 기존 발급건에 대한 쓰기를 막지 않고 회원별 최신 쿠폰 조회 인덱스를 생성합니다.
ALTER TABLE `issuances`
    ADD INDEX `idx_issuances_member_issued`
        (`member_id`, `issued_at` DESC, `id` DESC),
    ALGORITHM=INPLACE,
    LOCK=NONE;
