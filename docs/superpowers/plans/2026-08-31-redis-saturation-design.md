# Redis Saturation Design

## Goal

관제 saturation.resources의 Redis 행이 고정 N_A 대신 운영 Redis
10.4.3.119:6379의 실제 메모리 사용률과 원천 상태를 표시한다.

## Data flow

redis_exporter:v1.89.0이 운영 Redis의 INFO memory를 읽고
redis_memory_used_bytes와 redis_total_system_memory_bytes를 노출한다.
Prometheus는 exporter를 job=redis로 1초마다 수집한다. API는 다른 관제 지표와
마찬가지로 Prometheus만 조회하고 Redis를 직접 추가 조회하지 않는다.

Redis의 maxmemory=0은 운영 용량 정책이므로 이번 변경에서 임의 수정하지 않는다.
사용률은 redis_exporter 공식 mixin과 같은 used_memory / total_system_memory * 100으로
정의하고 화면 보조 문구에 used ÷ system memory를 명시한다.

## Failure semantics

- exporter가 정상이고 두 메모리 지표가 있으면 VALID 또는 시간에 따라 STALE이다.
- exporter가 살아 있지만 분자나 분모가 없으면 PENDING이다.
- Redis exporter의 up이 0이거나 Prometheus 질의가 실패하면 UNAVAILABLE이다.
- 원천 부재를 0%로 바꾸지 않는다.

## Deployment

루트 compose에 exporter를 추가하고 Prometheus 설정을 갱신한다. API 이미지를 다시
빌드하여 app1/app2만 재생성한다. production batch와 테스트 batch는 변경하지 않는다.

