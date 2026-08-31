# Main Environment Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Diagnose the issuance latency, fast-forward the deployed backend to the latest origin/main, and apply newly introduced environment variables without losing current deployment values.

**Architecture:** Preserve the existing working tree and runtime configuration, compare the fetched main branch rather than guessing variable names, then merge only required keys into the root deployment .env and docker-compose-v2.yml. Rebuild and recreate only services whose image or environment changed, followed by focused health and configuration checks.

**Tech Stack:** Git, Docker Compose, Spring Boot, MySQL/InnoDB, Prometheus

**Spec:** User request in this session (2026-08-31)

## Global Constraints

- Preserve all existing uncommitted source and deployment changes.
- Do not expose secret values in command output.
- Run only focused verification; do not run the full test suite.

---

### Task 1: Confirm latency root cause

**Files:**
- Inspect: storage/src/main/java/com/kafkick/storage/db/coupon/repository/CouponStockJpaRepository.java
- Inspect: core coupon issue service
- Inspect: /home/student/coupon-yaho/.env

- [ ] Confirm the single-row stock update and transaction boundary.
- [ ] Compare the load-test concurrency with active Tomcat and Hikari limits.
- [ ] Query MySQL/Prometheus evidence when available and state the root cause separately from amplifiers.

### Task 2: Synchronize latest main safely

**Files:**
- Preserve: all paths shown by git status --short

- [ ] Fetch origin/main and inspect incoming commits/files.
- [ ] Fast-forward with a reversible local-change preservation workflow only if required.
- [ ] Verify that the original local modifications remain present after synchronization.

### Task 3: Apply new deployment variables

**Files:**
- Modify: /home/student/coupon-yaho/.env
- Modify: /home/student/coupon-yaho/docker-compose-v2.yml
- Reference: .env.example
- Reference: compose.yml

- [ ] Diff environment-variable keys before and after the fetched main revision.
- [ ] Add only missing required keys, retaining existing values such as BATCH_ADMIN_TOKEN.
- [ ] Validate the rendered compose configuration without printing secrets.

### Task 4: Rebuild, recreate, and verify

**Files:**
- Build from: backend Gradle/Docker build definitions changed by main
- Deploy with: /home/student/coupon-yaho/docker-compose-v2.yml

- [ ] Run only the focused tests for incoming source changes, if applicable.
- [ ] Rebuild affected API/batch images and recreate affected containers.
- [ ] Verify API, batch :9091, actuator/Prometheus targets, and effective non-secret environment keys.

