CREATE TABLE coupon_round_schedule_guard (
    id TINYINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT chk_coupon_round_schedule_guard_singleton CHECK (id = 1)
) ENGINE=InnoDB;

INSERT INTO coupon_round_schedule_guard (id) VALUES (1);
