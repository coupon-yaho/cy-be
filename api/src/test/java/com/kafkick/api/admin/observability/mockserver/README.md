# 관제 프론트 연동용 목 서버

인프라 없이 실제 `AdminMetricsResponse`와 `PromMetricsAssembler`를 거친 응답을 제공합니다.

```bash
./gradlew :api:mockServer
./gradlew :api:mockServer --args='--port 18081'
MOCK_PORT=18081 ./gradlew :api:mockServer
```

기본 포트는 `18080`이며 loopback 인터페이스에만 바인딩됩니다. 프론트의 `VITE_ADMIN_API`를
`http://localhost:18080`으로 설정하세요.
예를 들어 `GET /api/v1/admin/metrics?window=1m`로 호출합니다. `window`는 `1m`, `5m`,
`15m` 또는 `ONE_MINUTE`, `FIVE_MINUTES`, `FIFTEEN_MINUTES`를 받습니다.

## 시나리오

- 파라미터 없음: 120초 동안 상승, 평탄, 감쇠, 유휴 파형을 반복합니다. 유휴 구간은 트래픽 5종의 `NO_TRAFFIC`과 값 `0.0`을 보여줍니다.
- `scenario=loaded`: 약 5,000 rps의 흔들리는 평탄 구간을 고정합니다.
- `scenario=idle`: 유휴 구간을 고정합니다.
- `scenario=stale`: 관측 시각을 300초 전으로 보내 `STALE`을 보여줍니다.
- `scenario=promDown`: 원천 질의를 실패시켜 조립기의 값별 `UNAVAILABLE` 격리를 보여줍니다.
- `scenario=budget`: 첫 질의를 늦춰 응답 예산 뒤쪽 질의가 `UNAVAILABLE`이 되는 모습을 보여줍니다.

시나리오는 각 폴링 요청의 쿼리 파라미터에서 읽으므로 서버 재시작 없이 즉시 바뀝니다.

## 보장하지 않는 범위

이 서버는 인증을 검사하지 않으므로 `X-User-Id`가 필요하지 않습니다. 또한
`GlobalExceptionHandler` 경로의 오류 본문과 게이트웨이 오류를 재현하지 않습니다. 따라서 프론트의
HTML 오류 응답 파싱 방어는 계속 필요합니다.

## 필드별 구현 상태

이 목은 실제 조립기를 그대로 태우므로 아래 상태를 실제 서버와 함께 따라갑니다. 아직 안 만든 것과
안 만들기로 한 것을 한 목록에 섞지 않습니다.

| 필드 | 상태 | 근거 |
| --- | --- | --- |
| `meta` | 구현 완료 | CY-416 |
| `errors` | 구현 완료 | CY-448 |
| `totalRps` | 만들지 않기로 함 | 프론트가 폐기한 필드 (OBS-38) |
| `clientInvalid` | 구현 완료 | CY-448 의 `errors.classes[].key` 로 흡수됨. 최상위 필드로는 만들지 않습니다 |
| `percentileMode` | 만들지 않기로 함 | 프론트에 대응 필드 없음 (OBS-38) |
| `series` | 진행 중 | OBS-33 |
| `markers` | 진행 중 | OBS-34 |
| `dependencies` | 원천 대기 | 미터 미배선 — Redis는 OBS-10, Kafka는 OBS-17. 조립기가 `PENDING`을 반환 |
| `persistence` | 원천 대기 | Kafka persist lag 미터는 OBS-17. 조립기가 `PENDING`을 반환 |

`만들지 않기로 함`은 결손이 아니라 폐기 결정입니다. 이 표를 보고 다시 만들지 마세요.
