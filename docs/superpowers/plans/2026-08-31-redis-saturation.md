# Redis Saturation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** 운영 Redis의 실제 메모리 사용률을 관리자 saturation 응답에 제공한다.

**Architecture:** 외부 Redis를 직접 폴링하지 않고 전용 redis_exporter를 Prometheus에 연결한다. API는 exporter의 사용량·시스템 메모리·신선도·up 표본을 기존 saturation 질의에 합쳐 상태가 있는 사용률로 조립한다.

**Tech Stack:** Docker Compose, Prometheus 3.5, redis_exporter 1.89.0, Java 21, Spring Boot 4.1, JUnit 5, AssertJ

**Spec:** docs/superpowers/plans/2026-08-31-redis-saturation-design.md

## Global Constraints

- 운영 Redis 주소는 10.4.3.119:6379이다.
- Redis maxmemory와 데이터는 변경하지 않는다.
- 관리자 API는 Redis를 직접 읽지 않고 Prometheus만 읽는다.
- 원천 부재나 장애를 0%로 표시하지 않는다.
- 관련 테스트만 실행한다.

---

### Task 1: Redis saturation 응답 계약

**Files:**
- Modify: api/src/test/java/com/kafkick/api/admin/observability/PromMetricsAssemblerTest.java
- Modify: api/src/test/java/com/kafkick/api/admin/observability/MetricAggregationTest.java
- Modify: api/src/main/java/com/kafkick/api/admin/observability/MetricAggregation.java
- Modify: api/src/main/java/com/kafkick/api/admin/observability/PromMetricsAssembler.java
- Modify: api/src/main/java/com/kafkick/api/admin/observability/dto/AdminMetricsResponse.java

**Interfaces:**
- Consumes: redis_memory_used_bytes, redis_total_system_memory_bytes, up with job=redis
- Produces: saturation.resources Redis utilization and detail used ÷ system memory

- [ ] **Step 1: Write failing behavior tests**

Add tests that feed one exporter instance with literal values 2000 used and 8000 total and assert Redis utilization is 25.0, state is VALID, and detail names the denominator. Add separate cases asserting missing capacity is PENDING and Redis exporter down is UNAVAILABLE. Extend the query contract test to require the two Redis metrics and job=redis.

- [ ] **Step 2: Verify RED**

Run ./gradlew :api:test --tests '*PromMetricsAssemblerTest' --tests '*MetricAggregationTest'.

Expected: FAIL because the Redis row is still fixed N_A and the saturation query omits Redis metrics.

- [ ] **Step 3: Add metric names and aggregation**

Add constants for redis_memory_used_bytes, redis_total_system_memory_bytes, redis.memory.utilization, and redis.memory.freshness.age.seconds. Register the two derived metrics with MAX aggregation.

- [ ] **Step 4: Assemble Redis usage and source status**

Extend saturationQuery with a job=redis selector for used, total, and up plus a named time-minus-timestamp freshness series. Derive Redis freshness from that age and the Redis job's up value. Replace the fixed notApplicable row with the existing per-instance percent helper using used as numerator and total system memory as denominator. Change only the Redis detail to used ÷ system memory.

- [ ] **Step 5: Verify GREEN**

Run the same two related test classes and require exit 0.

### Task 2: Exporter and Prometheus wiring

**Files:**
- Modify: /home/student/coupon-yaho/docker-compose-v2.yml
- Modify: /home/student/coupon-yaho/prometheus.yml

**Interfaces:**
- Consumes: redis://10.4.3.119:6379
- Produces: redis-exporter:9121/metrics and Prometheus job=redis

- [ ] **Step 1: Add the exporter service**

Add oliver006/redis_exporter:v1.89.0 with REDIS_ADDR=redis://10.4.3.119:6379, REDIS_EXPORTER_INCL_SYSTEM_METRICS=true, and REDIS_EXPORTER_REDIS_ONLY_METRICS=true. Do not expose it on the host and do not attach it to the local Redis service.

- [ ] **Step 2: Add the scrape target**

Add a Prometheus job named redis targeting redis-exporter:9121 at /metrics.

- [ ] **Step 3: Validate configuration**

Run docker compose config --quiet and promtool check config. Both must exit 0.

### Task 3: Build, deploy, and live verification

**Files:**
- Runtime image: my-spring-app:v2
- Runtime containers: redis-exporter, prometheus, app1, app2

**Interfaces:**
- Consumes: Tasks 1 and 2 artifacts
- Produces: live Redis saturation value in /api/v1/admin/metrics?window=3s

- [ ] **Step 1: Build the API image**

Run the repository's API Docker build for my-spring-app:v2 and require exit 0.

- [ ] **Step 2: Recreate only affected services**

Start redis-exporter, recreate Prometheus, and recreate app1/app2. Do not recreate batch, batch-clean, batch-corrupt, or the local Redis container.

- [ ] **Step 3: Verify exporter and Prometheus**

Assert exporter metrics contain non-zero redis_memory_used_bytes and redis_total_system_memory_bytes; verify Prometheus target job=redis is up and both instant queries return a sample.

- [ ] **Step 4: Verify the original symptom**

Call the admin metrics endpoint with X-User-Role ADMIN and window=3s. The Redis row must have numeric utilization, state VALID, and detail used ÷ system memory.

- [ ] **Step 5: Review the final diff and runtime scope**

Confirm only the planned backend/config files changed for this feature and production batch plus test services were not recreated.

