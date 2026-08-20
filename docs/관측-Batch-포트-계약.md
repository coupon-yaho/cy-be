# 관측·Batch 포트 계약

## 포트 배정

| 대상 | 포트 | 용도 | 호스트 공개 |
|---|---:|---|---|
| API | 8080 | 사용자·관리자 업무 API | 필요 |
| API | 9090 | Actuator·Prometheus scrape | 공개하지 않음 |
| Batch | 9091 | API가 내부 호출하는 verify 업무 API | 공개하지 않음 |
| Batch | 9092 | Actuator·Prometheus scrape | 공개하지 않음 |
| Prometheus | 9090 | PromQL HTTP API·UI | 운영 정책에 따름 |

API와 Prometheus의 9090은 서로 다른 컨테이너에서 사용한다. 주소가 각각
`api:9090`, `prometheus:9090`이므로 충돌하지 않는다.

## 접근 흐름

| 호출자 | 대상 | 주소 | 목적 |
|---|---|---|---|
| 외부 클라이언트 | API | `배포 호스트/DNS:8080` | 업무·관리자 API 호출 |
| 관제 API | Batch | `batch:9091`의 verify 트리거 | 검증 작업 실행 |
| Prometheus | API | `api:9090/actuator/prometheus` | API 지표 수집 |
| Prometheus | Batch | `batch:9092/actuator/prometheus` | Batch 지표·생존 상태 수집 |
| 관제 API | Prometheus | `prometheus:9090/api/v1/query*` | PromQL 실행 |

## Batch 관측 범위

Batch 관리 포트에서 다음 지표를 제공한다. Gauge 구현은 OBS-5가 담당하고 OBS-20은
Actuator와 Prometheus 노출 통로만 제공한다.

| 지표 | 산출 위치 |
|---|---|
| 정합성 gap 4종 | Batch JVM |
| 대기열 길이 | Redis ZCARD 조회 결과 |
| 재고 잔량 | Redis 재고 카운터 조회 결과 |
| Kafka persist lag | AdminClient의 committed offset과 end offset 차이 |

## 배포 주의사항

- API 관리 포트 9090을 호스트 `ports`에 매핑하지 않는다.
- Batch 업무 포트 9091을 호스트 `ports`에 매핑하지 않는다. 브라우저는 API만 호출한다.
- Batch 관리 포트 9092를 호스트 `ports`에 매핑하지 않는다.
- Batch의 9091은 verify 업무 포트이고 9092는 관측 전용 관리 포트다.
- Prometheus는 내부 네트워크에서 `api:9090`, `batch:9092`를 scrape한다.
