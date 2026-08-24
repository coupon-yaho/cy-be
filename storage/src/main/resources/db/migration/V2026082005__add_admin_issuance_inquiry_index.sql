ALTER TABLE `issue_attempts`
    ADD INDEX `ix_issue_attempts_member_occurred_id`
        (`member_id`, `occurred_at` DESC, `id` DESC),
    ALGORITHM=INPLACE,
    LOCK=NONE;
