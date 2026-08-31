# UTC Native Timestamps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Keep persistence and comparison on UTC while preventing MySQL session KST from writing future DATETIME(6) values that break the admin overview.

**Architecture:** Preserve the existing Instant/JDBC UTC contract and KST-only business schedule presentation. Make the coupon timestamp updates use MySQL's UTC clock, then repair only timestamp values that violate the no-future invariant.

**Tech Stack:** Java 21, Spring Data JPA, MySQL 8, Flyway, JUnit 5, AssertJ, Testcontainers

**Spec:** User-approved minimal UTC persistence direction from the 2026-08-31 production incident.

## Global Constraints

- Do not change KST coupon schedule or admin display semantics.
- Do not change JVM, JDBC, or Spring Batch UTC contracts.
- Preserve unrelated working-tree changes, especially .env.example.
- Repair only coupon_stocks.updated_at and issuances.updated_at values later than the UTC database clock.

---

### Task 1: Protect coupon timestamp writes from DB session timezone

**Files:**
- Modify: storage/src/test/java/com/kafkick/storage/db/coupon/repository/CouponIssueRepositoryTest.java
- Modify: storage/src/test/java/com/kafkick/storage/db/coupon/repository/CouponUseRepositoryTest.java
- Modify: storage/src/main/java/com/kafkick/storage/db/coupon/repository/CouponStockJpaRepository.java
- Modify: storage/src/main/java/com/kafkick/storage/db/coupon/repository/IssuanceJpaRepository.java

- [ ] Write failing MySQL integration tests that set the borrowed session to +09:00, perform the real update, and assert the stored DATETIME is bounded by UTC times.
- [ ] Run ./gradlew :storage:test --tests '*CouponIssueRepositoryTest' --tests '*CouponUseRepositoryTest' --rerun-tasks and verify RED by approximately nine hours.
- [ ] Replace only the three coupon-domain native SQL calls from CURRENT_TIMESTAMP(6) to UTC_TIMESTAMP(6).
- [ ] Re-run the focused tests and verify GREEN.

### Task 2: Repair already-future coupon timestamps

**Files:**
- Create: storage/src/main/resources/db/migration/V2026083101__repair_future_coupon_timestamps.sql
- Test: storage/src/test/java/com/kafkick/storage/db/migration/FutureCouponTimestampRepairMigrationTest.java

- [ ] Write a failing real-MySQL migration test with past and future rows.
- [ ] Run the focused test and verify RED because the migration resource is absent.
- [ ] Add guarded updates that set only future coupon_stocks.updated_at and issuances.updated_at values to UTC_TIMESTAMP(6).
- [ ] Re-run the focused test and verify GREEN while preserving past timestamps.

### Task 3: Deploy and verify the production symptom

- [ ] Run focused storage tests, ./gradlew :api:test :storage:test, and git diff --check.
- [ ] Rebuild my-spring-app:v2; recreate app1/app2 and nginx.
- [ ] Confirm both health endpoints are UP, Flyway 2026083101 succeeded, both upstreams work, and repeated authenticated overview requests return 200 without new future-stock exceptions.
