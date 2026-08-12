통신사 브랜드 데이 — 선착순 쿠폰 발급 시스템 PRD v4.15

Product Requirements Document · v4.15 확정본

# 통신사 브랜드 데이
대규모 트래픽 선착순 쿠폰 발급 시스템

12개 제휴 브랜드가 매월 여는 브랜드 데이에서, 재고 10,000장에 20,000명이 동시에 몰려도 초과 발급 0건을 보장하고 그것을 스스로 증명하는 시스템.

미결 사항 0건
팀 5명 · 15 영업일
Java 21 · Spring Boot 3.x
외부 입력 1개 — 착수일

01개요
02도메인
03아키텍처
04API · 화면
05측정
06실행 계획
07발표
08리스크 · 가정
09데이터 모델
10성능 · 확장
11보안

이 프로젝트가 무엇을 왜 만드는가. 채점 루브릭 5항목과 우선순위, 그리고 범위 밖으로 확정한 것들.

## 목표

12개 제휴 브랜드가 매월 여는 브랜드 데이에서, 재고 10,000장에 20,000명이 동시에 몰려도 초과 발급 0건 · 캠페인당 1인 1매를 보장하는 선착순 쿠폰 발급 시스템을, DB 비관적 락 → Redis 원자 카운터 → Kafka 비동기의 3단계로 점진 구현해 동일 조건에서 측정 비교하고, 300만 건 이력을 고의 오염 데이터셋으로 신뢰성을 증명한 결정론적 검증 체계로 보증한다.

0초과 발급

20K동시 요청

3M검증 대상 이력

600오염셋 검출 목표

## 채점 루브릭과 대응

| 항목 | 세부 기준 | 배점 | 대응 |

| 창의성 및 기획성 | 아이디어 / 현황 관련성 / 분석 목표 / 시장성 | 20 | 브랜드 데이 12개 + 멤버십 등급 4단계 + 할인 정책 3종 + 트래픽 현황 문서 |

| 기술성 및 완성도 | 실현 가능성 / 기술 이해도 / 완성도 / 교과 반영 / 해석 / Data 품질 | 30 | 3버전 동시성 사다리 + 결정론적 검증 + 현실 분포 100만/300만 |

| 프로젝트 수행능력 | 목표 달성도 / 문제 해결 / 산출물 품질 / 업무 분담 | 30 | 5영역 분담 + D5·D10·D15 게이트 + 협업 규칙 |

| 프레젠테이션 능력 | 전달력 / 시간 준수 / 완성도 | 10 | 발표 15분 + 영상 5분 = 20분 하드 캡, 리허설 2회 |

| 프론트엔드 개발 능력 | 프론트엔드 / 시각화 | 10 | 3화면 · 패널 24종 |

설계에 반영된 판단

동시성·정합성에 독립 배점이 없습니다. 우리가 가장 공들이는 영역은 기술성 30점에 흡수됩니다. 그래서 브랜드 데이·등급·통계로 창의성 20점과 프론트 10점을 따로 채웁니다.

등급 제한은 동시성 난이도를 올리지 않습니다. 같은 등급이 수십만 명이면 원래 문제와 같은 문제입니다. 넣는 이유는 기술이 아니라 도메인 현실성입니다. 그래서 JWT 클레임으로 처리해 발급 경로에 부하를 주지 않습니다.

## 과제 필수 요건

### 전제 조건

- 회원가입·로그인 구현 없음

- 개인정보 반드시 마스킹

- 유저 100만 + 이력 300만 적재

- 외부 연동 Mocking

### 발급 시스템

- 재고 10,000 / 동시 20,000

- 초과 0건, 1인 1매

- 부하 테스트로 재현·시연

- 오픈 예약 · 실시간 조회

### 정합성 검증

- 상태 4종 + 멱등성

- 이력↔재고 불일치 0

- 300만 전수 · 재실행 동일 결과

- 리포트 자동화

## 우선순위

Must 10 / Should 9 / Could 2. 잘라야 할 때는 Could부터, 그다음 Should 하단부터입니다.

| 등급 | 항목 | 근거 |

| Must | v1 동시성 제어 / 초과발급 0 / 1인 1매 | 없으면 과제 미달 |
``````
| Must | /entry + Entry-Token 검증 | /issue가 요구하므로 대기열 로직보다 먼저 |

| Must | v2 Redis Lua 원자 카운터 | v1만으로는 "비교"가 성립 안 함 |

| Must | 상태 전이 4종 + 멱등성 + 재고 불변식 | 과제 필수 |

| Must | 브랜드 12 · 캠페인 147 · 유저 100만 · 이력 300만 | 과제 필수 · Data 품질 |

| Must | 검증 배치 300만 전수 + 결정론 | 과제 필수 |

| Must | k6 부하 테스트 · PII 마스킹 · 외부 Mocking | 과제 필수 |

| Must | 등급 자격 검증 (JWT 클레임) | 창의성 20점. 동시성 영향 없음 |
``````````
| Must | 보안 필수 6건 — 관리 포트 분리 + role: ADMIN · Entry-Token 1회성 · alg 고정 · .gitignore · actuator 최소화 · 에러·로그 마스킹 | 합쳐 약 0.5일. /actuator/env가 AES 키를 노출하는 등 실제 구멍 |

| Should | v3 Kafka · 대시보드 14차트 · 오염셋 600건 · 스케줄러 · 통계 · 워밍업 · 대기열 3모드 · 측정 3조합 · Chaos 1종 | 차별화와 배점 |

| Could | Chaos 나머지 조합 · 측정 9조합 전수 | 여유 시 |

## 범위 밖 — 확정

| 분류 | 제외 | 근거 |

| 동시성 | v0 베이스라인, 낙관적 락, Redisson RLock 별도 버전 | 3버전으로 비교 서사 충분 |

| 등급 | 등급별 재고 쿼터 | 동시성 난이도를 올리지 않으면서 검증 배치·더미데이터·오염셋을 흔듦 |

| 도메인 | 회선당 발급 제한, 쿠폰 양도·선물 | 난이도 기여 없이 복잡도만 증가 |

| 대기열 | AIMD, Leaky bucket, Sliding window, 오픈 전 사전 예약 | 토큰버킷으로 충족 |

| 아키텍처 | 순수 이벤트 소싱, 클라우드 배포, 외부 피처 플래그 | 로컬 Compose + yml로 충족 |

| 프론트 | React 등 SPA 프레임워크 | 단일 HTML + Chart.js로 충족 |

쿠폰이 무엇인가. 브랜드 데이 구조, 상태 전이와 재고 불변식, 더미데이터 분포, 그리고 검증을 검증하는 오염 데이터셋.

## 브랜드 데이 구조

브랜드 데이는 반복 이벤트입니다. 캠페인에 반복 규칙을 직접 넣으면 재고 불변식이 회차 단위로 바뀌어 검증 체계 전체를 재설계해야 합니다. 그래서 캠페인은 단발로 두고 스케줄러가 찍어냅니다.

```
Brand (12개)
└─ coupon_templates 매월 N번째 요일 HH:MM · 재고 · 정책 · 참여 등급
└─ 스케줄러가 매일 자정 +30일치 생성
└─ Campaign ← 기존 구조 그대로. 불변식·동시성 로직 무손상

UNIQUE(template_id, open_at) -- 스케줄러 중복 실행 방어
```

## 브랜드 12개

| # | 브랜드 | 업종 | 브랜드 데이 | 정책 | 등급 |

| 1 | 모카빈 | 카페 | 1주 화 14:00 | 정률+상한 | 전체 |

| 2 | 씨네플러스 | 영화 | 1주 목 18:00 | 정액 | 전체 |

| 3 | 버거하우스 | 외식 | 1주 금 11:00 | 정률+상한 | 전체 |

| 4 | 프레시마트 | 마트 | 2주 화 10:00 | 정액 | SILVER↑ |

| 5 | 북스토리 | 서점 | 2주 수 15:00 | 정률+상한 | 전체 |

| 6 | 필름아레나 | 영화 | 2주 금 19:00 | 정액 | GOLD↑ |

| 7 | 스포츠존 | 스포츠 | 3주 월 12:00 | 정률+상한 | 전체 |

| 8 | 뷰티랩 | 뷰티 | 3주 수 16:00 | 정률+상한 | SILVER↑ |

| 9 | 딜리버리고 | 배달 | 3주 금 17:00 | 데이터 | 전체 |

| 10 | 트래블온 | 여행 | 4주 화 13:00 | 정액 | GOLD↑ |

| 11 | 게임패스 | 게임 | 4주 목 20:00 | 데이터 | 전체 |

| 12 | 헬스클럽 | 피트니스 | 4주 금 07:00 | 정률+상한 | VIP |

요일·시각을 흩뿌린 이유는 통계 화면의 요일×시간 히트맵이 의미 있는 패턴을 그리도록 하기 위함입니다. 참여 등급도 섞어 대시보드에서 필터 효과가 보이게 했습니다.

## 할인 정책 3종

### 정률 + 상한

20% 할인, 최대 20,000원
`min(금액 × 0.2, 20000)`

### 정액

5,000원 고정 할인

### 데이터

데이터 1GB 제공 (금액 무관)

## 멤버십 등급

| 등급 | 비중 | 인원 | 참여 가능 브랜드 |

| VIP | 5% | 50,000 | 12개 전체 |

| GOLD | 15% | 150,000 | 11개 |

| SILVER | 30% | 300,000 | 9개 |

| WELCOME | 50% | 500,000 | 8개 |

동시성에 부하를 주지 않는 이유

사전 발급 JWT 클레임에 등급을 담아두므로 DB 조회가 없습니다. 검증은 락 밖에서 문자열 비교 한 번으로 끝납니다.

```
{ "sub": "u_812934", "grade": "GOLD", "exp": 1799999999 }
```

## 쿠폰 상태 모델

```
ISSUED ──사용──→ USED
ISSUED ←사용취소── USED ← 역방향 허용 (구매 취소)
ISSUED ──취소──→ CANCELLED (종단, 재고 +1)
ISSUED ──만료──→ EXPIRED (종단, 재고 +1)
USED ──만료──→ ✗ 불가

재고 불변식: 잔여재고 = total_stock − (ISSUED 수 + USED 수)
```

역방향 전이가 있어야 재고가 양방향으로 움직이고, "사용취소 중복 요청 → 재고 이중 복원" 같은 실제 버그가 성립합니다. 멱등성 요구가 장식이 아니게 됩니다.

발급 자격 — 재발급 불가

한 사용자는 한 캠페인에서 평생 1회만 발급받습니다. 취소·만료 후에도 재발급 불가.

```
ALTER TABLE coupon ADD CONSTRAINT uk_campaign_user UNIQUE (campaign_id, user_id);
```

이 제약이 최종 방어선이 되어, v1/v2/v3 어느 버전에 버그가 있어도 1인 1매 위반이 물리적으로 불가능합니다.

## 캠페인 구성

```
[과거 캠페인 144개] 12 브랜드 × 12개월
└─ 캠페인당 재고 18,000 ~ 34,000장, 총재고 380만
└─ 발급 이력 300만 건 분배 → 평균 소진율 79%
└─ 캠페인별 소진율 60~100% 흩뿌림 (완판·미달 혼재)
└─ 전부 CLOSED

[현재 캠페인 3개] 다음 브랜드 데이, 재고 100%
#1 모카빈 재고 10,000 전체 공개 ← 부하테스트 기준 시나리오
#2 필름아레나 재고 3,000 GOLD↑
#3 딜리버리고 재고 2,000 전체 공개
```

총재고를 380만으로 잡은 이유

재고와 이력이 같으면 모든 캠페인이 100% 완판되어 검증 배치의 "잔여재고 > 0" 분기가 한 번도 실행되지 않습니다.

산술 검증 — 100만 유저 중 파레토 상위 30만이 이력의 80%(240만) 보유 → 인당 8건 ≈ 144개 중 8개 참여. 등급 상위 20%(VIP+GOLD 20만 명)가 이력 상위군과 자연스럽게 겹칩니다.

## 더미데이터 사양

| 유저 | 100만 명 · VIP 5 / GOLD 15 / SILVER 30 / WELCOME 50% |

| 활동 분포 | 파레토 — 상위 30만 명이 이력의 80% |

| 캠페인 | 과거 144 + 현재 3 = 147개 |

| 이력 | 300만 건 |

| 상태 분포 | ISSUED 40% / USED 35% / EXPIRED 15% / CANCELLED 10% |

| 복원 이력 | USED 중 20%가 사용 → 사용취소 → 재사용 |

| 복원 상태 | ISSUED 중 5%가 USED에서 복원된 건 |

| 만료 임박 | 1%가 24시간 내 만료 |

| 결정론 | 고정 난수 시드 + as-of 기준시각 |

### 시간축 상태 기울기

상태 분포를 균일 적용하면 1년 전 캠페인에 `ISSUED` 120만 건이 남는 부자연스러운 데이터가 됩니다.

| 캠페인 시기 | 상태 경향 | 비고 |

``
| 최근 1~2개월 | ISSUED 우세 | 만료 임박 1%가 여기 집중 |
``
| 3~6개월 전 | USED 우세 | |
``
| 7~12개월 전 | EXPIRED 우세 | 유효기간 경과 |

## 정합성 검증 2계층

| 계층 | 시점 | 동작 | 담당 |

````
| 실시간 드리프트 | 1초 주기 | Redis 카운터 ↔ DB 집계 대조. WARN ≥ 10, CRITICAL ≥ 100. 알람(Mock) + 대시보드 | ① |
``
| 배치 전수 검증 | 온디맨드 | 300만 건 전수, verify(as_of_ts) 결정론, 리포트 자동 생성 | ④ |

실시간 계층은 v2/v3에서만 의미가 있습니다 (v1은 Redis 카운터를 쓰지 않음). 부하 종료 후 드리프트는 0이어야 합니다 — 부하 중 일시적 차이는 v3의 비동기 특성상 정상입니다.

## 오염 데이터셋 — 600건

| # | 오염 유형 | 건수 |

``
| 1 | 재고는 줄었는데 history에 ISSUE 기록 없음 | 100 |
``````
| 2 | history는 USED인데 coupon.status는 ISSUED | 100 |
``
| 3 | CANCEL_USE가 2번 기록되어 재고 이중 복원 | 100 |
````
| 4 | 종단 상태(EXPIRED)에서 USED로 불법 전이 | 100 |

| 5 | 동일 쿠폰이 두 유저에게 발급됨 | 100 |

| 6 | 동일 유저가 같은 캠페인에서 2건 발급 | 100 |

합격 기준

오염 셋에서 정확히 600건 검출 AND 정상 셋에서 0건. `검증 결과 0건`만으로는 검증이 잘된 건지 아무것도 안 보는 건지 구분할 수 없습니다. 오염셋이 그걸 가르는 유일한 방법입니다.

## 스키마 규약

ERD를 그릴 때 의미가 갈리는 지점만 못박습니다. 나머지 컬럼 설계는 팀 재량입니다.

재고 카운터 이름

```
coupon_stocks(campaign_id PK, total_quantity, active_count)
```

`issued_count`라는 이름을 쓰지 마세요. 누적 발급 수로 읽히는데 우리 불변식은 그게 아닙니다.

`active_count` = 현재 `ISSUED` + `USED` 개수. 취소·만료 시 감소합니다. 이름을 잘못 잡으면 구현자가 십중팔구 누적으로 짜고 초과 발급 판정이 통째로 어긋납니다.

| 규약 | 이유 |

``
| total_quantity는 재고 행에만. 캠페인 행에 중복 금지 | 어긋나면 어느 쪽이 진실인지 판정 불가. 검증 대상이 하나 늘어남 |
````
| UNIQUE (campaign_id, member_id). limit_per_member 같은 컬럼을 두지 않음 | N매를 허용하면 UNIQUE를 걸 수 없어 최종 방어선이 사라짐 |
``
| 낙관적 락용 version 컬럼 없음 | v1은 비관적 락이고 낙관적 락은 범위 밖. 쓰지 않을 컬럼이 있으면 구현자가 혼란 |
``
| UNIQUE (template_id, open_at) | 스케줄러 중복 실행 시 캠페인 이중 생성 방어 |

### PII 컬럼 형태

```
members(
id, membership_grade,
email_enc VARBINARY(256), -- AES
email_hash CHAR(64), -- HMAC-SHA256, 검색·유니크용
phone_enc VARBINARY(256),
phone_hash CHAR(64)
)
UNIQUE KEY uk_email_hash (email_hash);
```

`UNIQUE`는 평문이 아니라 해시 컬럼에 겁니다. AES 암호문은 매번 값이 달라져 유니크 제약이 성립하지 않습니다. `member_id`는 내부 식별자라 암호화·마스킹 대상이 아닙니다 — 마스킹하면 검증 리포트에서 "어느 회원이 중복 발급됐는지"를 쓸 수 없습니다.

### 만료 시각 컬럼 구분

| 컬럼 | 소속 | 의미 |

````
| open_at / close_at | 캠페인 | 캠페인이 열리고 닫히는 시각 |
``
| valid_days | 템플릿 | 발급된 쿠폰이 며칠간 유효한지 |
````
| expires_at | 쿠폰 | issued_at + valid_days. 만료 판정의 유일한 기준 |

캠페인이 닫혀도(`CLOSED`) 이미 발급된 쿠폰은 `expires_at`까지 유효합니다.

동시에 2만 명이 누르면 어떻게 되는가. 3버전 사다리, 입장·발급 분리, 적응형 대기열과 서킷브레이커 계층 분리.

## 버전 사다리

### v1 · DB 비관적 락

`SELECT ... FOR UPDATE`

관찰 — 커넥션 풀 고갈, 락 대기 큐

### v2 · Redis Lua

원자 카운터 + 중복방지 SET

관찰 — 병목이 DB→Redis로 이동, Redis↔DB 정합성 리스크 발생

### v3 · Redis + Kafka

선점 후 비동기 영속화

관찰 — 스파이크 흡수, 최종 일관성, 백로그·DLQ

불변식 판정 기준

부하 종료 + Kafka 백로그 전량 소진 후 DB 기준. v3의 일시적 Redis/DB 발산은 위반이 아니지만, 영구 미영속 발급은 위반입니다.

## 적응형 대기열

대기열(인바운드)과 서킷브레이커(아웃바운드)는 방향이 반대인 보호 장치입니다. 계층을 분리하고, 만나는 지점을 게이트 하나로 좁혔습니다.

```
[진입 · 해제] 자기 부하 신호로만 판단

진입(OR): in-flight > 2,000 | p99 > 500ms | DB풀 > 80%
해제(AND): in-flight < 1,000 & p99 < 200ms & 대기 인원 == 0
앞 두 조건 30초 지속 + 대기열이 완전히 빈 뒤에만 해제
입장 속도: 토큰버킷 초당 500명 고정
플래핑방지: 히스테리시스 + 대기열 최소 유지 60초

[ADAPTIVE 내부 상태]

IDLE 부하 낮고 대기열 비어 있음 → 즉시 입장
QUEUEING 부하 초과 → 신규는 대기열 뒤로
DRAINING 부하 회복, 대기자 남음 → 신규도 여전히 대기열 뒤로
대기 인원 0 도달 시 IDLE 방출 속도만 상향

[게이트] 발급이 불가능하면 신규 입장을 막는다

해당 버전의 필수 서킷브레이커가 OPEN이면 신규 입장 차단
v1 → dbCB
v2 → redisCB 또는 dbCB
v3 → redisCB 또는 dbCB

차단 시 /entry 는 503 + Retry-After
```

게이트는 rate 조절이 아니라 on/off입니다. 발급이 아예 불가능한 상태에서 사람을 줄 세우는 건 의미가 없으므로 입장 자체를 막습니다. 등급 미달·기발급자를 대기 없이 거절하는 것과 같은 부류의 사전 차단입니다.

HALF_OPEN은 대기열과 연결하지 않는다

half-open의 프로브는 "의존성이 살아났는지 확인"이고 입장 rate는 "감당 가능한 속도로 흘려보내기"라 목적이 다릅니다. 프로브 1건이 성공하면 CB가 닫히는데 그 사이 초당 N명을 입장시킬 이유가 없습니다. 프로브는 CB 내부 관심사로 둡니다.

### 서킷브레이커는 의존성별로 3개

하나로 묶으면 무엇이 고장났는지 구분할 수 없고, 아래 규칙 8·9의 거동 차이가 성립하지 않습니다.

| CB | 감싸는 호출 | 관계된 버전 |

``
| redisCB | Redis 재고 카운터, 대기열 자료구조 | v2 · v3 |
``
| dbCB | DB 락, 전 버전 영속화 | v1 · v2 · v3 |
``
| kafkaCB | Kafka 프로듀서 | v3 |

인바운드 in-flight는 직접 계측합니다 — 서블릿 필터의 `AtomicInteger` 또는 Micrometer `http.server.requests.active`. Resilience4j `Bulkhead`는 아웃바운드 동시성 제한기이므로 그 지표를 인바운드 판단에 쓰면 인바운드 정책이 아웃바운드 설정 변경에 딸려 흔들립니다. Bulkhead는 아웃바운드 보호 용도로만 씁니다.

모든 임계치는 `application.yml` 외부화 — 부하 테스트 중 재기동 없이 튜닝합니다.

## 입장과 발급의 분리

대기열은 발급 페이지에 들어갈 때 거칩니다. 발급 요청을 줄 세우는 게 아니라 페이지 입장을 통제합니다.

```
1. 페이지 진입
POST /api/v1/campaigns/{id}/entry
├─ 등급 미달 → 403 GRADE_NOT_ELIGIBLE ← 대기 없이 즉시
├─ 이미 발급받음 → 409 ALREADY_ISSUED ← 대기 없이 즉시
├─ 대기열 비활성 → 200 { admitted, entryToken, expiresIn: 180 }
└─ 대기열 활성 → 202 { queueToken, position, etaSeconds }

2. 대기 중 순번 조회 (202인 경우)
GET /api/v1/campaigns/{id}/queue (폴링, 1초 간격)
→ { position, etaSeconds } … → { status: ADMITTED, entryToken }

3. 발급 버튼
POST /api/v1/campaigns/{id}/issue (헤더: Entry-Token)
→ 201 Created 또는 409 STOCK_EXHAUSTED
```

| # | 설계 규칙 | 이유 |

````
| 1 | /issue는 유효한 Entry-Token 없이 호출 불가 | 대기열 우회 차단 |
````
| 2 | 입장 ≠ 발급 보장. 재고가 먼저 소진되면 409 | 재고 판정은 /issue 시점 |
``
| 3 | entryToken TTL 180초, 만료 시 슬롯 반납 | 이탈 사용자의 슬롯 점유 방지 |

| 4 | 등급 미달·기발급자는 대기 없이 즉시 거절 | 1,500번 기다린 끝에 거절당하는 UX 방지 |
````
| 5 | /entry 중복 호출은 기존 queueToken 반환 (멱등) | 새로고침으로 순번이 밀리지 않도록 |

| 6 | 토큰버킷 rate = 초당 입장 허용 인원 (고정 500/s) | 부하 신호는 진입·해제만 결정하고 rate는 건드리지 않음 |
``
| 7 | /entry는 대기열 모드와 무관하게 항상 존재 | 토글되는 건 대기열 로직이지 엔드포인트가 아님 |
````
| 8 | dbCB OPEN → 둘 다 503. 대기열 상태는 Redis에 남으므로 복구 후 순번 유지 | 신규 유입만 차단 |
``
| 9 | redisCB OPEN → v2/v3 발급 불가. 대기열 자체가 Redis 기반이라 대기열 우회, 전량 503, 순번 유지 시도 안 함. v1은 정상 동작 | 지킬 수 없는 약속을 명세에 두지 않기 위해 |
``
| 10 | 대기자가 한 명이라도 있으면 OFF로 전환하지 않는다. 수동 전환·CB 복구에도 동일 적용 | 신규 요청이 대기자를 추월하는 순번 역전 차단 |
````
| 11 | 순번은 시각이 아니라 INCR로 만든 단조 증가 queue_seq | 동일 밀리초 요청과 서버 시계 오차로 순서가 흔들리지 않도록 |

### 순번 역전 방어

대기열에서 가장 위험한 결함은 나중에 온 사람이 먼저 받는 것입니다. 우리 구조에서 생기는 지점은 두 곳입니다.

지점 1 — 대기열 해제 순간

대기자 199명이 남아 있는데 부하 지표가 회복돼 `OFF`로 전환되면, 방금 도착한 사람이 즉시 입장해 199명을 추월합니다.

→ 해제 조건에 `대기 인원 == 0`을 AND로 넣습니다. 대기자가 하나라도 있으면 `DRAINING` 상태로 계속 흘려보냅니다. 조건 검사로 막는 게 아니라 상태 정의로 불가능하게 만드는 방식입니다.

지점 2 — 순번을 시각으로 매기는 것

Sorted Set score를 타임스탬프로 쓰면 같은 밀리초 요청의 순서가 불확정이고, 서버가 여러 대면 시계 오차만큼 순서가 흔들립니다.

```
queue_seq = INCR queue:{campaignId}:seq -- 원자적, 전역 단조 증가
ZADD queue:{campaignId} {queue_seq} {userId}
```

### 선착순의 정의

입장 순서는 `queue_seq`로 보장합니다.

발급 순서는 `/issue` 도착 순서입니다.

입장은 순서를 보장하지만 발급을 보장하지 않습니다.

1번으로 입장했어도 170초 뒤에 누르면, 500번으로 입장해 즉시 누른 사람이 먼저 받습니다. 티켓팅에서 입장이 좌석을 보장하지 않는 것과 같습니다. 이 정의가 없으면 "먼저 눌렀는데 왜 못 받았나"에 답할 기준이 없습니다.

### 대기열 자료구조

| 키 | 타입 | 용도 |

````
| queue:{campaignId}:seq | String (INCR) | 단조 증가 순번 발급기 |
````
| queue:{campaignId} | Sorted Set | score = queue_seq, member = userId. 순번 = rank |
``
| queue:{campaignId}:member | Set | 중복 진입 방지 |
``
| entry:{campaignId}:{userId} | String (TTL 180s) | entryToken |

부하 테스트 영향
전 구간 폴링입니다. 20,000 VU에 SSE를 붙이면 부하 테스트 클라이언트가 먼저 죽고, 대시보드도 SSE로는 과거 구간을 되감을 수 없습니다. 프로젝트에 `SseEmitter`를 쓰지 않습니다.

## 캠페인 상태머신과 워밍업

```
SCHEDULED ──(open_at)──→ OPEN ──→ CLOSED
↑ 재고 소진 또는 close_at

오픈 전 /entry · /issue 모두 409 NOT_OPENED + Retry-After + open_at
T-30초 워밍업 — Redis 재고 카운터, 대기열 Sorted Set·Set 생성, 캐시 예열
오픈 순간 상태 전환만으로 즉시 개시
```

왜 409인가

`425 Too Early`는 RFC 8470에서 TLS early data 재전송을 거절하는 용도라 "아직 오픈 전"과 의미가 다릅니다. 캠페인 상태와 요청이 충돌한 상황이므로 `409`가 맞고, 재시도 시점은 `Retry-After` 헤더와 본문의 `openAt`으로 전달합니다.

오픈 첫 병목은 /entry

20,000명이 `/issue`가 아니라 `/entry`부터 몰립니다. 워밍업이 재고 카운터만 예열하면 오픈 첫 초에 대기열 키 생성이 폭주합니다.

오픈 시각은 상대 시각입니다 — `시드 실행 시각 + 5분`. 절대 시각을 박으면 그 시점이 지난 뒤 오픈런을 재현할 수 없습니다. 시드를 재실행하면 `SCHEDULED`로 리셋됩니다.

## Chaos 통과 기준

| 구분 | 기준 |

| 정합성 | 초과 발급 0건 / 장애 종료 후 검증 불일치 0건 — 무조건 |
``
| 장애 중 응답 | CB 오픈 5초 → half-open. 503에 재시도 안내를 반드시 실을 것. 무응답·타임아웃 불가 |

| 복구 | Redis 재기동 후 10초 / Kafka 백로그 60초 / DB 5초 내 정상화 |

| 시나리오 | 감지 CB | v1 | v2 | v3 | 비고 |

``
| DB 커넥션 끊기 | dbCB | 필수 | ✓ | ✓ | v1의 핵심 시나리오. 대기열 순번은 Redis에 남아 복구 후 유지 |
``
| Redis 강제 종료 | redisCB | ➖ | 여유 시 | 여유 시 | v1은 Redis 미사용이라 정상 동작. v2/v3는 대기열까지 불능이라 우회 후 전량 503 |
``
| Kafka Consumer 지연 | kafkaCB | ➖ | ➖ | 여유 시 | v3 전용. 백로그 소화 60초 목표 |

Redis를 죽이면 v1만 살아남는다

세 버전의 의존성 구조 차이가 그대로 드러나는 지점입니다. 여유가 생기면 이 시나리오를 우선 추가하세요 — 발표에서 "왜 v1이 여전히 필요한가"에 대한 답이 됩니다.

전부 k6 지표와 검증 배치로 자동 판정합니다. 수동 육안 확인 기준은 두지 않습니다.

어떤 요청에 무엇으로 답하는가. 응답 코드 규약이 측정과 Chaos 판정의 근거이고, 대시보드는 독자별로 3화면입니다.

## API

공통 헤더 `Authorization: Bearer <사전 발급 HS256 JWT>` · 상태 변경은 `Idempotency-Key`

| Method | Path | 설명 |

``
| GET | /api/v1/brands | 브랜드 12개 + 다음 브랜드 데이 일정 |
``
| GET | /api/v1/campaigns | 캠페인 목록 (상태·재고·오픈 시각·참여 등급) |
``
| GET | /api/v1/campaigns/me | 내 등급으로 참여 가능한 캠페인만 |
``
| POST | /api/v1/campaigns/{id}/entry | 발급 페이지 입장 |
``
| GET | /api/v1/campaigns/{id}/queue | 내 순번·예상 시간. 폴링 1초 |
````
| POST | /api/v1/campaigns/{id}/issue | 쿠폰 발급 — Entry-Token 필수 |
``
| POST | /api/v1/coupons/{id}/use | 쿠폰 사용 |
``
| POST | /api/v1/coupons/{id}/cancel-use | 사용 취소 (복원) |
``
| POST | /api/v1/coupons/{id}/cancel | 쿠폰 취소 |
``````
| CRUD | /api/v1/admin/brands · /schedules · /campaigns | 관리자 화면 |
``
| POST | /api/v1/admin/verify?asOf={ts} | 검증 배치 실행 |
``
| GET | /api/v1/admin/stats | 통계 집계 결과 |
``
| GET | /actuator/admission-capacity | 발급 계층이 발행하는 입장 여력. 대기열이 소비하고 대시보드가 관측 |
``
| GET | /api/v1/admin/metrics?window= | 관제 시계열. 폴링 1초 |
``
| GET | /api/v1/admin/benchmarks | 버전별 측정 결과 |

## 응답 코드 규약 ⭐

이 표가 없으면 측정이 불가능합니다

k6가 이 코드로 성공 / 정상 실패 / 진짜 에러를 구분하고, Chaos 자동 판정과 에러율 집계가 모두 여기에 의존합니다. 임의로 바꾸지 마세요.

| 상황 | 코드 | 본문 | k6 집계 |

``
| 입장 즉시 허용 | 200 | admitted, entryToken, expiresIn | 입장 |
``
| 대기열 진입 | 202 | queueToken, position, etaSeconds | 대기 |
``
| 발급 성공 | 201 | couponId, policy, expiresAt | 성공 |
``
| 등급 미달 | 403 | GRADE_NOT_ELIGIBLE | 정상 실패 |
``
| 입장 토큰 없이 발급 | 403 | NO_ENTRY_TOKEN | 에러 |
``
| 입장 토큰 만료 또는 이미 사용됨 | 403 | ENTRY_TOKEN_EXPIRED | 정상 실패 |
``
| 재고 소진 | 409 | STOCK_EXHAUSTED | 정상 실패 |
``
| 이미 발급받음 | 409 | ALREADY_ISSUED | 정상 실패 |
````
| 오픈 전 | 409 | NOT_OPENED + Retry-After | 정상 실패 |
``
| 캠페인 마감 | 409 | CAMPAIGN_CLOSED | 정상 실패 |
``
| 만료 쿠폰 사용 | 409 | COUPON_EXPIRED | 정상 실패 |
``
| 불법 상태 전이 | 409 | INVALID_TRANSITION | 정상 실패 |

| 멱등키 재요청 | 200 | 최초와 동일 본문 | 성공 (중복 집계 안 함) |
``
| 멱등키 충돌 | 422 | IDEMPOTENCY_KEY_REUSED | 에러 |
``````
| dbCB 오픈 | 503 | TEMPORARILY_UNAVAILABLE, dependency: DB | 열화 |
``````
| redisCB 오픈 (v2/v3) | 503 | dependency: REDIS, queueBypassed: true | 열화 |
````
| kafkaCB 오픈 (v3) | 503 | dependency: KAFKA | 열화 |

| 그 외 서버 오류 | 5xx | | 에러 |

원칙 3가지

- `409`와 등급 미달 `403`은 전부 정상 실패 — 비즈니스 규칙 거절이지 장애가 아닙니다

- `202`는 성공도 실패도 아닙니다 — 대기열 접수 상태, 별도 카운터

- `GETDEL`이 `nil`을 반환하는 두 경우(만료 / 이미 사용)를 같은 코드로 묶습니다. 클라이언트 대응이 `/entry`부터 다시로 동일해 구분할 이유가 없고, 구분하면 토큰 상태를 추측할 단서를 주게 됩니다

- Chaos 중 `503`은 허용, 무응답·타임아웃은 불가 — "503 대신 안내 응답"은 코드를 바꾸라는 게 아니라 본문에 재시도 안내를 실으라는 뜻입니다

## 멱등성 설계

``````
| 적용 대상 | 상태 변경만 — use, cancel-use, cancel. 발급은 제외 |
``
| 발급 제외 이유 | UNIQUE(campaign_id, user_id)가 이미 중복을 물리적으로 차단 |
``
| 키 생성 | 클라이언트가 UUID v4 생성, 헤더 Idempotency-Key |
``
| 재요청 | 같은 키 + 같은 해시 → 저장된 응답을 200 OK로 반환 |

| 동시 요청 | 키 유니크 제약으로 선착순 1건만 처리, 나머지는 완료 대기 후 저장된 응답 반환 |

| 보관 | 24시간, 이후 배치 정리 |

## 대시보드 — 독자별 3화면 · 패널 26종

화면을 데이터 출처가 아니라 대상 독자로 나눕니다

드리프트 같은 인프라 지표는 캠페인을 만드는 운영자에게 무의미합니다. 반대로 브랜드별 전환율은 개발자가 볼 이유가 없습니다. 한 화면에 섞으면 운영자는 노이즈에 묻히고 개발자는 흩어진 지표를 모아야 합니다.

| 화면 | 독자 | 묻는 질문 |

| ① 캠페인 운영 | 비즈니스 | 이벤트가 잘 돌아가고 있나 |

| ② 시스템 관제 | 엔지니어링 | 시스템이 정확하고 버티고 있나 |

| ③ 분석 · 비교 | 발표 | 구조가 어떻게 달랐나 |

| # | 설계 원칙 | 이유 |

| 1 | 시간축이 있으면 선(line). 막대는 퍼널·랭킹에만 | 선착순은 변화율이 정보. 막대는 순간값만 |
``
| 2 | 폴링 1초. SSE를 쓰지 않는다 | Grafana도 실제로는 주기적 쿼리. SSE는 과거를 되감을 수 없어 시간범위 선택과 충돌하고, SseEmitter 코드가 통째로 사라짐 |

| 3 | 이벤트 스트림은 커서 폴링 + 링버퍼 | 안 하면 브라우저가 먼저 죽음 |
````
| 4 | 시간 범위 1m/5m/15m · 갱신 1s/5s/off | 시연 중 되감기가 가능해야 함 |

### 화면 ① 캠페인 운영 비즈니스 — 10 패널

운영자가 판단에 쓰는 것만 둡니다. `p99`·`DB풀`·`드리프트`는 여기 없습니다.

캠페인 운영 · 모카빈 브랜드 데이Last 5m1s

잔여 재고

6,412

/ 10,000

발급 진행률

35.9%

3,588장

초당 발급

1,847

peak 2,310

대기 인원

3,204

예상 12초

캠페인 상태

OPEN

14:00 오픈

사용률

41.2%

발급 대비

⑤ 캠페인별 재고 소진

모카빈 6,412필름아레나 1,102딜리버리고 941

⑥ 상태별 보유량 — 쓰이는가 · 취소되는가 · 만료되는가

ISSUEDUSEDCANCELLEDEXPIRED

⑨ 발급 이벤트 스트림+ 1,847건 생략

14:02:31.442 m_812934 모카빈 MOCA-8F2K 201 발급 VIP

14:02:31.443 m_119287 모카빈 — 409 소진

14:02:31.443 m_772341 모카빈 — 403 등급미달 WELCOME

14:02:31.444 m_004512 모카빈 — 202 대기 #1,847

14:02:31.445 m_338190 모카빈 MOCA-2J7Q 201 발급 GOLD

⑩ 상태 전이 스트림

14:05:12.001 MOCA-8F2K ISSUED → USED 주문 #88213

14:05:44.882 MOCA-3B1P USED → ISSUED 주문 취소

14:06:02.114 MOCA-9K2X ISSUED → CANCELLED 사용자 취소

14:10:00.000 MOCA-1A2B ISSUED → EXPIRED 만료 배치

14:11:18.220 MOCA-7E4F ISSUED → USED 주문 #88291

⑨·⑩이 운영자에게 가장 실질적인 패널입니다. "분명히 눌렀는데 안 됐다"는 선착순 이벤트 최다 문의이고, 이 둘이 그 답을 직접 보여줍니다.

### 화면 ② 시스템 관제 엔지니어링 — 9 패널

| # | 패널 | 형태 | 임계 |

````
| 11 | 초과 발급 | stat | 0 녹색 · >0 적색 경보 |
````
| 12 | p99 응답시간 | stat | >500ms 주황 · >1000ms 적색 |
``````
| 13 | Circuit Breaker | 배지 ×3 | redisCB dbCB kafkaCB |
``````````
| 14 | 응답 결과 rate | line ×5 | 201·202·409·403·5xx |
``````
| 15 | 응답시간 분위 | line ×3 | p50·p95·p99 |

| 16 | 리소스 사용률 | line ×4 다축 | in-flight · DB풀% · Redis 지연 · Kafka 랙 |

| 17 | 상태 전이 rate | line ×5 | 만료 배치가 스파이크로 튐 |

| 18 | 대기열 | line ×2 + 배경 밴드 | 모드가 배경색으로 깔림 |

| 19 | Redis ↔ DB 격차 | line + 임계선 | 버전마다 의미가 다름 ↓ |

🔴 19번은 버전마다 다른 지표입니다

| 버전 | 라벨 | 정상값 | 판정 |

| v1 | — | — | 패널 비활성. "v1은 발급 경로에 Redis 미사용" |
``````````
| v2 | drift | 0 | 임계 WARN 10·CRITICAL 100 유효. 0이 아니면 진짜 오류 — DECR 성공 후 DB INSERT 실패로 재고가 영구 손실 |
``
| v3 | backlog | 0이 아닌 게 정상 | 임계선 없음. 설계상 Redis가 앞서고 DB가 따라옴 |

v3에 `WARN 10 / CRITICAL 100`을 적용하면 부하 중 내내 적색입니다. 백로그가 수천 건 쌓이는 게 정상이니까요. 항상 빨간 패널은 아무도 안 봅니다 — 경보 피로로 v2의 진짜 문제를 놓칩니다.

```
v3 에서 봐야 할 것 — 절대값이 아니라

기울기가 양수로 유지 → 컨슈머가 못 따라감
부하 종료 후 0 수렴 → 60초 목표. 수렴 지점을 세로선으로 표시
수렴하지 않음 → 영구 미영속 = 위반
```

v2에서는 오류 감지기, v3에서는 최종 일관성 증명기입니다. v3의 수렴 곡선은 "비동기여도 결국 맞는다"를 보여주는 시연 소재입니다.

### 화면 ③ 분석 · 비교 발표 — 7 패널

| # | 패널 | 형태 | 무엇을 말하나 |

| 20 | 버전별 재고 소진 곡선 | line ×3 | v1·v2·v3를 같은 시간축에 겹침 |

| 21 | 버전별 p99 추이 | line ×3 | 부하 구간 전체의 지연 변화 |

| 22 | 버전별 DB 풀 사용률 | line ×3 | v1 병목의 시각적 증거 |

| 23 | 버전 비교표 | table | 발표 슬라이드에 그대로 캡처 |

| 24 | 브랜드별 월별 발급 추이 | line ×12 | 어느 브랜드가 성장·쇠퇴하는가 |

| 25 | 요일 × 시간 히트맵 | heatmap 7×24 | 브랜드 데이 요일이 도드라지는가 |

| 26 | 상태 전이 퍼널 | 퍼널 | 발급 → 사용 / 취소 / 만료 비율 |

20~22가 발표 4:00–9:00 구간의 정적 그래프 출처입니다. 24는 12개월 시계열이 있어 막대가 아니라 선이고, 26만 시간축이 없어 다른 형태를 씁니다.

### 이벤트 스트림 — 커서 폴링

🔴 링버퍼가 없으면 브라우저가 먼저 죽습니다

```
GET /api/v1/admin/events?since=<cursor>&limit=50

{ "events": [ … 최대 50건 … ],
"nextCursor": 184722,
"dropped": 1847 ← 그사이 링버퍼에서 밀려난 건수 }
```

| 서버 버퍼 | 최근 200건 링버퍼. 넘치면 오래된 것부터 밀어냄 |

| 일시정지 | 클라이언트가 커서를 안 올리면 끝. 별도 API 불필요 |
``
| dropped | 반드시 표시. 숨기면 관전자가 전량이라고 오해 |
``
| 표시 규칙 | 회원은 member_id만, 쿠폰은 앞 8자리. 이름·연락처 절대 금지 |

| 집계 패널 | ⑤~19는 이 제한과 무관. 카운터라 이벤트 수를 다 반영 |

SSE였다면 일시정지에 별도 제어가 필요했습니다. 커서 폴링은 커서를 멈추는 것만으로 정지되고, 재개하면 그사이 `dropped`가 그대로 나옵니다.

### 못 끝내면 뒤에서부터 자릅니다

| 등급 | 패널 | 근거 |

| 필수 | ① · ⑤ · ⑥ · 11 · 19 | 과제 핵심 조건(초과 발급 0 · 재고 불변식 · 정합성)의 증거 |

| 권장 | ⑨ · ⑩ 이벤트 스트림 · 14 · 18 | 이벤트 스트림이 영상에서 가장 잘 먹힙니다 |

| 보통 | ②~④ · ⑦ · ⑧ · 12 · 13 · 15~17 | 화면을 채우는 맥락 지표 |

| 여유 | 20~23 버전 비교 | 정적 캡처로 대체 가능 |

| 최후 | 24~26 분석 | 전부 포기해도 과제 조건에 영향 없음 |

필수 5개만 남겨도 과제 요건은 충족합니다. 화면 ③부터 자르세요.

## 통계 집계 전략

매 요청마다 300만 건을 집계할 수 없습니다. 사전 집계 테이블 + 배치로 처리합니다.

```
campaign_stats(campaign_id, issued, used, cancelled, expired, sold_out_seconds)
grade_stats(campaign_id, grade, issued, used)
hourly_stats(day_of_week, hour, issued)
```

검증 배치와 같은 패스에서 만듭니다

어차피 300만 건을 전수 스캔하므로 집계까지 함께 산출하면 추가 비용이 거의 없습니다. "왜 사전 집계인가"를 발표에서 설명할 수 있는 것이 차트를 다섯 개 더 그린 것보다 점수에 가깝습니다.

무엇을 재고 무엇으로 합격을 판정하는가. 정합성 지표는 합격/불합격, 성능 지표는 병목 해석용.

## 측정 원칙

정합성 지표는 합격/불합격, 성능 지표는 비교·해석용입니다. 과제가 "처리량은 평가하지 않는다"고 했으므로 성능 수치는 순위가 아니라 "병목이 어디로 이동했는가"의 근거입니다.

## 정합성 지표 — 전부 0이어야 합격

| 지표 | 측정 방법 |

``
| 초과 발급 건수 | COUNT(status IN ('ISSUED','USED')) − total_stock 가 0 이하. 누적 발급 수가 아니라 동시 보유 수 기준 |
``
| 캠페인당 1인 초과 발급 | GROUP BY campaign_id, user_id HAVING COUNT(*) > 1 |

| 이력 ↔ 재고 불일치 | 검증 배치 결과 |

| 미영속 발급 (v3) | Redis 선점 수 − DB 영속 수 (백로그 소진 후) |
````
| 등급 자격 위반 발급 | coupons JOIN campaigns JOIN members JOIN grades 후 (eligible_grades_mask & bit_value) = 0 |

누적 발급 수로 재면 안 됩니다

취소·만료가 재고를 복원하므로 누적 이력은 `total_stock`을 정상적으로 초과합니다.

```
total_stock = 10,000
100명 발급 → 취소 (재고 100 복원)
다른 100명이 그 재고 발급
─────────────────────────────
누적 발급 이력 = 10,100 ← 정상
ISSUED + USED = 10,000 ← 불변식 만족
```

더미데이터에 `CANCELLED` 10%(30만 건)가 있어 누적 기준이면 정상 데이터에서 대량 오탐이 발생하고, 오염셋 합격 기준(정상 셋 0건)이 원천 불가능해집니다.

## 성능 지표

측정 기준 통일

모든 시간 지표는 클라이언트가 성공 응답을 받은 시점 기준. v3를 Redis 선점 기준으로 재면 부당하게 빨라 보입니다.

| 지표 | 비고 |

| 재고 소진 소요 시간 | 첫 요청 ~ 마지막 성공 응답. 버전 비교 대표 지표 |

| 응답시간 p50 / p95 / p99 / max | p99 기준 비교 |
``````
| 에러율 | 5xx·타임아웃만. 409·403 정상 실패는 제외 |

| DB 반영 완료 시각 (v3) | 마지막 성공 응답 ~ Kafka 백로그 소진 |

### 서버 지표

| 지표 | 관찰 대상 |

| HikariCP 풀 사용률 / 대기 시간 | v1 병목의 핵심 증거 |

| 락 대기 시간 | v1 |

| Redis 명령 지연 | v2 |

| Kafka 프로듀서 지연 / 컨슈머 랙 | v3 병목의 핵심 증거 |

| in-flight 요청 수 | 대기열 임계치 입력 |

## 버전 비교표 — 발표 산출물

| 버전 | 모드 | 소진 시간 | p95 | p99 | TPS | 에러율 | DB풀 | DB 지연 | 초과 | 1인초과 | 불일치 |

``
| v1 | OFF | | | | | | | — | 0 | 0 | 0 |
``
| v2 | OFF | | | | | | | — | 0 | 0 | 0 |
``
| v3 | OFF | | | | | | | | 0 | 0 | 0 |

- 위 3행이 필수입니다. `OFF`여야 세 버전 차이가 순수하게 동시성 제어 방식만 남습니다

- 여유가 있으면 `ALWAYS`·`ADAPTIVE` 행을 추가해 대기열이 어느 부하 구간부터 이득인가를 증명합니다

- 모드 컬럼 없이 기록하지 마세요. 어느 모드에서 잰 수치인지 안 남으면 비교가 무의미해집니다

측정 조건 고정 — 동일 Docker Compose 리소스 limit, 동일 k6 스크립트, 동일 초기 데이터, 동일 측정 기준, 동일 캠페인(#1 모카빈).

누가 언제 무엇을 하는가. 5영역 분담, D1~D15 게이트, 테스트 전략.

## 역할 분담 — 5영역

담당자 배정은 팀에서 정합니다. D1~D2는 전원이 ①을 공동 구축해 인터페이스를 고정한 뒤 갈라집니다.

| 영역 | 범위 | 산출물 |

| ① 도메인·코어 | 엔티티/ERD, 상태 전이, 멱등성, 브랜드·스케줄·스케줄러, 등급 검증, 만료 3계층, 실시간 드리프트 감지 | 도메인 모듈, 스케줄러, API 스펙 |

| ② 동시성 v1+v2 | DB 비관적 락, Redis Lua 원자 카운터, k6 시나리오 분담 | v1/v2 발급 전략 |
``````
| ③ 인프라·비동기 | /entry + Entry-Token, v3 Redis+Kafka, 적응형 대기열, Resilience4j | /entry, v3, 대기열 레이어 |

| ④ 데이터·검증 | 더미데이터 생성기, 검증 배치, 오염셋 600건, 리포트, 통계 집계 | 시드 모듈, 검증·집계 배치 |

| ⑤ 시각화·측정 | 대시보드 3화면 14차트, 관리자 CRUD, k6, Chaos 하네스, 시연 영상 | 대시보드, 관리자 화면, k6, 영상 |

부하 재균형 — 필수

- D6~D10 · ①이 실시간 드리프트를 맡는다 — ①은 공동 설계 후 여유가 생기고 ④는 4개가 몰림

- D6~D10 · ②가 k6 시나리오를 분담한다 — ⑤는 대시보드 폴링 연결에 집중해야 함

- D11~D15 · 영상 편집은 ⑤ 단독으로 두지 않는다 — 촬영은 ⑤, 편집은 2인 1조

## 마일스톤 — D1 ~ D15

외부 입력 1개
착수일을 `D1`에 대입하면 전 일정이 확정됩니다. D는 영업일 기준이며 주말은 건너뜁니다.

D1–D5Week 1 · 기반과 v1

D1~D2 전원 공동 — ERD·상태머신·API 스펙 확정, 브랜드 12개 + 스케줄 규칙 설계, Docker Compose 골격(리소스 limit), `IssuanceStrategy` 인터페이스 고정

``
| ① | 엔티티 + 상태 전이 + 멱등성 + UNIQUE(campaign_id, user_id) + 등급 검증 |

| ② | v1 비관적 락 |
``````
| ③ | /entry 최소 버전 (항상 200 admitted = 대기열 OFF) |

| ④ | 더미데이터 생성기 — 브랜드 12 · 캠페인 147 · 유저 100만 · 이력 300만 적재 |

| ⑤ | k6 스크립트 골격 + 대시보드 스켈레톤 (mock 데이터로 렌더링 검증) |

D5 게이트도메인 인터페이스 고정 · `/entry`→`/issue` 흐름 동작 · v1이 1,000 동시에서 초과발급 0건 · 더미데이터 적재 완료 · 대시보드 스켈레톤 렌더링

D6–D10Week 2 · v2/v3와 검증

| ① | 만료 3계층 + 브랜드 데이 스케줄러 + 실시간 드리프트 감지 |

| ② | v2 Redis Lua 완성 + k6 시나리오 분담 |
``
| ③ | v3 Kafka 비동기 + 대기열 로직을 /entry에 탑재 + CB 통합 |

| ④ | 검증 배치 300만 전수 + 오염셋 600건 + 리포트 + 통계 집계 테이블 |

| ⑤ | 대시보드 폴링 연결 + 이벤트 스트림 + 관리자 CRUD + Chaos 하네스 |

⚠️ D8 판단 지점v3가 막히면 v2까지로 축소하고 v3는 설계·PoC 발표로 전환. D11 이후로 미루지 않는다.

D10 게이트v1·v2가 20,000 풀스케일 통과 · v3 완성 · 검증 배치가 오염셋 600건 정확 검출 · 대시보드 실시간 동작 · 스케줄러가 캠페인 자동 생성

D11–D15Week 3 · 증명과 마감

``
| D11–12 | Chaos DB 커넥션 끊기 × v1 실행 + 복구 검증, 측정 3조합 수집, 통계 화면 완성 — 이때 화면 녹화 동시 진행 |

| D13 | 측정 데이터 확정, 리포트, 기획 서사 문서, 발표 자료, Q&A 대비 |

| D14 | 영상 편집 (5분) + 리허설 1회 |

| D15 | 리허설 1회 + 수정 반영 + 버퍼 |

D15 게이트20분 하드 캡 준수 실측 확인 · 전 산출물 완성

D5 게이트를 1,000 동시로 낮춘 이유 — D1~D2를 설계에 쓰면 실개발은 3일입니다. 그 안에 20,000 풀스케일까지 요구하는 건 비현실적이고, 정합성 로직의 정확성은 1,000 동시로 충분히 검증됩니다.
영상 촬영을 D11~D12로 당긴 이유 — Chaos를 실제로 실행하는 그 순간이 촬영 기회입니다.

## 협업 규칙

"업무 분담"이 수행능력 30점의 명시 세부항목입니다. 프로세스 자체가 채점 증빙입니다.

````
| 브랜치 | main 보호. feat/<영역번호>-<설명> |

| PR | 머지 전 리뷰어 최소 1명 승인. 셀프 머지 금지 |

| 커밋 | Conventional Commits |

| 이슈 | 작업 단위 이슈 등록 + 담당자·영역 라벨 → 업무 분담 증빙 |

| 스탠드업 | 매일 15분. 어제 / 오늘 / 막힌 것. 기록을 남긴다 |

| 게이트 | D5·D10·D15 점검 회의. 통과 여부를 문서에 기록 |

## 테스트 전략

`교과 내용 반영 수준`이 기술성 30점의 명시 세부항목입니다. 테스트 전략 자체가 득점 요소입니다.

| 계층 | 도구 | 대상 |

| 단위 | JUnit 5 + AssertJ | 할인 계산, 상태 전이, 등급 자격, 불변식 |
``
| 동시성 | JUnit 5 + CountDownLatch | 발급·상태 변경 경합. 이 프로젝트의 핵심 |

| 통합 | Testcontainers | MySQL·Redis·Kafka를 테스트마다 격리 기동 |

| 부하 | k6 | 20,000 동시 시나리오 |

```
// 재고 N에 2N개 스레드가 동시에 돌진 → 정확히 N건만 성공해야 함
int stock = 100, threads = 200;
var latch = new CountDownLatch(1);
var done = new CountDownLatch(threads);
var pool = Executors.newFixedThreadPool(threads);

IntStream.range(0, threads).forEach(i -> pool.submit(() -> {
latch.await(); // 전원 대기
issueService.issue(campaignId, userId(i));
done.countDown();
}));
latch.countDown(); // 동시 발사
done.await();

assertThat(successCount).isEqualTo(stock);
assertThat(couponRepo.countByCampaign(campaignId)).isEqualTo(stock);
```

| # | 반드시 커버할 시나리오 | 기대 결과 |

| 1 | 재고 N에 2N 동시 발급 | 정확히 N건, 초과 0 |
``
| 2 | 동일 유저가 동시에 10회 발급 | 1건만 성공 (UNIQUE) |
``
| 3 | 같은 쿠폰에 cancel-use 동시 5회 | 재고 1회만 복원 |

| 4 | 같은 멱등키로 동시 10회 상태 변경 | 1회 반영, 나머지 동일 응답 |

| 5 | 발급과 만료 배치 동시 실행 | 불변식 유지 |
````
| 6 | 같은 유저가 /entry 동시 10회 | 1건 등록, 동일 queueToken |
``````
| 7 | Entry-Token 없이 /issue | 403 NO_ENTRY_TOKEN |

| 8 | 스케줄러 동시 2회 실행 | 캠페인 1건만 생성 |
``````
| 9 | redisCB OPEN 상태에서 /entry 동시 호출 | 대기열 우회, 전량 503. 순번 생성 시도 없음 |
````
| 10 | dbCB OPEN → 복구 후 대기 유저의 /queue 조회 | 순번 보존, 이어서 입장 가능 |
````
| 11 | 대기자 100명 남은 상태에서 부하 지표가 해제 조건 충족 | OFF 전환 안 됨. DRAINING 유지, 신규도 대기열 뒤로 |
````
| 12 | 같은 밀리초에 /entry 1,000건 동시 도착 | queue_seq 1,000개가 모두 다르고 단조 증가 |
````````
| 13 | 같은 entryToken으로 동시에 10회 /issue | GETDEL이 원자적이라 1회만 통과, 나머지 9건은 403 ENTRY_TOKEN_EXPIRED |

20분 안에 무엇을 보여주는가. 영상 5분은 움직여야 설득되는 것에, 발표 15분은 비교 서사에.

## 발표 15분 + 영상 5분 = 20분 하드 캡

Q&A는 별도. 합계 20분을 절대 초과할 수 없습니다. 여유가 0이므로 리허설을 D14·D15에 분산해 실측합니다.

## 시연 영상 5분 — 사전 녹화

| 구간 | 장면 | 목적 |

| 0:00–0:30 | 인트로 — 시스템 구성도, 브랜드 데이 개념 | 맥락 |

| 0:30–2:00 | 재고 소진 + 초과발급 0건 — 20,000 동시 중 재고 10,000→0, 초과 카운터 0 유지 | 과제 핵심 조건 증명 |

| 2:00–3:30 | Chaos 주입과 복구 — DB 커넥션 끊기 → 안내 응답 → 5초 내 정상화, 정합성 무손상 | 시각적 임팩트 |

| 3:30–5:00 | 고의 오염 검출 — 정상 300만에서 0건, 오염셋에서 600건 정확 검출 | 검증이 작동함을 증명 |

촬영 조건 — 고정

````
| 대기열 모드 | OFF — ADAPTIVE면 입장이 초당 500명으로 제한되어 곡선이 밋밋해짐 |

| 버전 | v2 또는 v3 — 소진이 빨라 2분 안에 담김. v1은 락 대기로 느림 |

| 캠페인 | #1 모카빈 (재고 10,000, 전체 공개) |

## 발표 15분

| 구간 | 내용 | 배점 |

| 0:00–2:00 | 문제 정의 — 통신사 브랜드 데이의 실제 트래픽 특성, 시장성 | 창의성 20 |

| 2:00–4:00 | 도메인 설계 — 브랜드·캠페인·등급·상태머신·재고 불변식 | 창의성 20 |

| 4:00–9:00 | v1 → v2 → v3 점진 고도화와 측정 비교 (정적 그래프) | 기술성 30 |

| 9:00–11:00 | 정합성 검증 체계 — 결정론적 검증과 오염셋으로 검증을 검증 | 기술성 30 |

| 11:00–13:00 | 적응형 대기열 — 인바운드/아웃바운드 보호의 계층 분리와 의존성별 CB | 기술 30 창의 20 |

| 13:00–14:00 | 확장 방향 — 측정이 가리키는 다음 병목과 트리거 (성능·확장 탭 참조) | 기술 30 창의 20 |

| 14:00–15:00 | 역할 분담, D1~D15 게이트 달성 이력, 회고 | 수행능력 30 |

축소 금지 구간

`v1/v2/v3 비교`는 영상에서 제외했으므로 발표 슬라이드가 유일한 전달 경로입니다. 4:00–9:00을 줄이지 마세요. 그래프는 사후 비교 화면(차트 7~9)에서 캡처합니다.

## Q&A 대비 — D13 준비

| 예상 질문 | 답변 근거 |

| 낙관적 락은 왜 안 썼나요? | 선착순은 경합률이 100%에 가까워 재시도 폭주로 오히려 불리. 비교 대상 우선순위가 낮았음 |

| Redisson 분산락 대신 Lua를 쓴 이유는? | 락 획득·해제 왕복 대신 원자 연산 한 번으로 끝내는 편이 선착순 카운터에 적합. 락 실패·타임아웃 처리도 불필요 |

| v3에서 성공을 알렸는데 DB 저장이 실패하면? | Redis 선점이 발급 확정이며 Kafka가 재처리·DLQ로 최종 영속 보장. 불변식은 백로그 소진 후 DB 기준 |

| 등급 제한이 동시성에 무슨 의미가 있나요? | 의미 없습니다. 같은 등급이 수십만 명이면 원래 문제와 같은 문제입니다. 등급은 도메인 현실성을 위한 것이고, 그래서 JWT 클레임으로 처리해 발급 경로에 부하를 주지 않았습니다 |

| 등급별로 재고를 나누지 않은 이유는? | 동시성 난이도를 올리지 않으면서 검증 배치·더미데이터·오염셋에 파급됩니다. 이득 대비 비용이 맞지 않아 범위에서 제외했습니다 |

| 왜 대기열을 항상 켜지 않나요? | 서버가 감당 가능한 구간에서는 불필요한 대기가 손해. ADAPTIVE가 그 판단을 자동화하고 OFF/ALWAYS 비교로 이득 구간을 수치로 제시 |

| 서킷브레이커가 대기열 속도를 조절하나요? | 아닙니다. 대기열 진입·해제는 in-flight·p99·DB풀이라는 자기 부하 신호로만 판단하고, CB는 발급이 아예 불가능할 때 신규 입장을 막는 on/off 게이트로만 씁니다. 두 메커니즘은 방향이 반대라(인바운드 vs 아웃바운드) 정책을 섞지 않았습니다 |
``
| 대기열이 꺼질 때 먼저 온 사람이 밀리지 않나요? | 대기자가 0이 되기 전에는 꺼지지 않습니다. 부하가 회복돼도 DRAINING 상태로 남아 신규 요청까지 계속 대기열 뒤로 보냅니다. 조건 검사가 아니라 상태 정의로 불가능하게 만들었습니다 |
``
| 먼저 눌렀는데 왜 못 받았나요? | 입장 순서는 queue_seq로 보장하지만 입장이 발급을 보장하지는 않습니다. 입장 후 늦게 누르면 먼저 입장한 사람도 소진될 수 있습니다 |
````
| CB를 왜 3개나 두나요? | 무엇이 고장났는지 구분해야 대응이 갈리기 때문입니다. dbCB가 열리면 대기열 순번은 Redis에 남아 복구 후 유지되지만, redisCB가 열리면 대기열 자체가 Redis 기반이라 순번을 지킬 수 없습니다. CB 하나로 묶으면 이 구분이 불가능합니다 |

| 검증 배치가 0건이면 정말 문제가 없나요? | 그 질문 때문에 오염 데이터셋 600건을 만들었습니다. 정확히 600건을 잡아내므로 "0건"이 신뢰할 수 있는 0건입니다 |

| 처리량이 왜 이것밖에 안 나오나요? | 로컬 Compose에 리소스 limit을 고정해 세 버전을 같은 조건에서 비교하는 것이 목적. 절대 성능이 아니라 병목의 이동을 봅니다 |

무엇이 어긋날 수 있고 그때 어떻게 하는가. 조기 신호와 사후 대응을 분리했고, 가정에는 검증 방법을 붙였습니다.

## 리스크 — 우선순위 매트릭스

| | 영향 큼 | 영향 중간 |

| 확률 높음 | R1 v3 미완 · R2 폴링 렌더 지연 | R9 ⑤ 과부하 · R6 차트 미완 |

| 확률 중간 | R3 20분 초과 | R4 적재 지연 · R7 코어 충돌 |

| 확률 낮음 | R12 관리 API 노출 | R5 · R8 · R10 · R11 |

좌상단 4건(R1·R2·R3·R9)에 관리 비용을 집중합니다. 나머지는 조기 신호만 지켜봅니다.

## 리스크 등록부

각 항목에 조기 신호를 붙였습니다. "이게 보이면 리스크가 현실화되는 중"이라는 뜻이고, 완화책이 발동하는 시점입니다.

| # | 리스크 | 조기 신호 | 사전 완화 | 사후 대응 | 소유 |

| R1 | v3가 D10까지 미완성 | D8까지 Kafka 컨슈머가 단건도 소화 못 함 | D8 판단 지점 명시. v3를 D6에 착수 | v2까지로 축소, v3는 설계 발표로 전환. D11 이후로 안 미룸 | ③ |
``
| R2 | 1초 폴링으로 24패널을 매번 전체 렌더링하면 브라우저가 버벅임 | D7에 패널 10개를 넘기며 갱신이 눈에 띄게 끊김 | Chart.js update('none')로 애니메이션 제거 · 데이터 포인트 300점 롤링 · 값이 바뀐 패널만 갱신 | 갱신 주기를 2~5초로 완화. Grafana 기본도 5초라 손실이 작다 | ⑤ |

| R3 | 20분 하드 캡 초과 | D14 리허설에서 18분 초과 | 영상 5분 고정. 리허설 D14·D15 분산 | 대기열 구간을 슬라이드 1장으로 압축 | 전원 |

| R4 | 300만 적재·검증 시간 초과 | D4까지 적재가 안 끝남 | JDBC batch. D3~D5 미리 적재 | 이력 100만으로 축소, 분포 비율은 유지. 미달을 문서에 명시 | ④ |

| R5 | 루브릭 공통 가정이 틀림 | — (확인 불가) | 대시보드는 데이터의 출력면이라 전달력으로 회수 | 대응 불필요. 투자 손실 없음 | — |

| R6 | 패널 24종 D12까지 미완 | D11에 통계 5종 착수도 못 함 | 우선순위 고정 — 실시간 6 > 비교 3 > 통계 5 | 통계 차트부터 포기. 9종으로 마감 | ⑤ |

| R7 | 도메인 코어 충돌 | D3에 같은 파일 PR 충돌 2회 이상 | D1~D2 전원 공동으로 인터페이스 고정 | 코어 담당이 머지 게이트키퍼 | ① |

| R8 | 캠페인 147개가 검증에 파급 | D3 검증 쿼리에 캠페인 루프 발생 | 불변식이 캠페인 단위라 파급 없음을 D3 확인 | 과거 캠페인 48개로 축소, 캠페인당 재고 증대 | ④ |

| R9 | ⑤ 영역 과부하 | D9에 ⑤ 미완 항목 3개 이상 | 부하 재균형표 실행 — ②가 k6, ①이 드리프트 | 영상 편집 2인 1조. 통계 차트 포기 | ⑤ |
````````
| R10 | 3단계 흐름이 k6를 복잡하게 | D5에 k6가 /entry→/issue 연결 실패 | 대기열 OFF로 먼저 측정 | OFF 측정만으로 비교표를 채우고 대기열은 시연으로 | ②⑤ |
````
| R11 | 스케줄러 캠페인 중복 생성 | 동일 open_at 캠페인 2건 | UNIQUE(template_id, open_at) | 제약 위반 로그로 즉시 발견. 중복 삭제 후 재실행 | ① |
````
| R12 | 관리 API·Actuator 무인증 노출 | 코드 리뷰에서 /admin/* 인증 필터 부재 | 관리 포트 분리 + role: ADMIN | Compose에서 관리 포트 노출 제거만으로 즉시 차단 | ③ |

## 확정된 가정

미결 사항은 없습니다. 아래는 검증할 수 없어 가정으로 고정한 항목이며, 각각 검증 방법과 무효화 시 대응을 붙였습니다.

가정을 적어두는 목적은 "우리가 모르는 것을 안다"를 남기는 것입니다. 검증 방법이 없는 가정은 그냥 희망이므로, 확인할 방법이 없으면 무효화 시 대응이라도 반드시 적습니다.

| # | 가정 | 검증 방법 | 시점 | 무효화 시 대응 |

| A1 | 루브릭은 공통이며 프론트 10점이 적용된다 | 없음 | — | 대시보드는 데이터의 출력면이라 손실 없음. 전달력으로 회수 |

| A2 | 백엔드 1명이 AI 보조로 대시보드 구현 가능. SSE를 안 쓰므로 난이도가 크게 낮음 | 목 데이터로 24패널을 1초 갱신해 렌더 성능 확인 | D6 | 갱신 주기 완화 + 통계 패널 포기 |

| A3 | 브랜드 데이 추가가 동시성·검증에 파급 안 됨 | 검증 쿼리가 캠페인 루프 없이 도는지 확인 | D3 | 과거 캠페인 48개로 축소 |

| A4 | 등급 검증이 발급 경로에 부하를 주지 않음 | 등급 검증 on/off 응답시간 비교 | D5 | 설계상 보장되어 무효화 여지 낮음 |

| A5 | 2 vCPU에서 v1이 수백 RPS를 낸다 | D5 게이트의 1,000 동시 측정 | D5 | 스레드·커넥션 재산정. 비교 자체는 성립 |

| A6 | 로컬 Compose로 팀원 간 환경 재현 | 전원이 같은 시드로 적재해 행 수 대조 | D2 | 기준 측정 머신 1대 지정 |

도메인 규칙이 테이블로 어떻게 내려앉는가. 불변식을 물리 제약으로 표현하는 것이 원칙입니다.

## 설계 원칙

### 물리 제약 우선

불변식은 애플리케이션이 아니라 DB 제약으로 표현한다. 로직에 버그가 있어도 막혀야 한다

### 이력은 append-only

상태는 갱신, 이력은 추가만. 둘을 대조하는 것이 정합성 검증 자체

### 발급 조건 스냅샷

템플릿이 바뀌어도 이미 발급된 쿠폰의 조건은 변하지 않는다

## 전체 구조

```

erDiagram
BRANDS ||--o{ COUPON_TEMPLATES : "운영"
COUPON_TEMPLATES ||--o{ CAMPAIGNS : "스케줄러가 회차 생성"
CAMPAIGNS ||--|| COUPON_STOCKS : "재고 1:1"
CAMPAIGNS ||--o{ COUPONS : "발급"
MEMBERS ||--o{ COUPONS : "보유"
GRADES ||--o{ MEMBERS : "등급 코드"
COUPONS ||--o{ COUPON_HISTORIES : "상태 전이 이력"
COUPONS ||--o{ COUPON_USAGES : "사용·취소 실적"
VERIFICATION_RUNS ||--o{ VERIFICATION_FINDINGS : "검출 항목"

GRADES {
varchar code PK
tinyint bit_value
}
BRANDS {
bigint id PK
varchar name
varchar category
}
COUPON_TEMPLATES {
bigint id PK
bigint brand_id FK
varchar policy_type
int discount_rate
int max_discount_amount
int discount_amount
int data_grant_mb
int valid_days
tinyint nth_week
varchar day_of_week
time start_time
int stock_per_occurrence
tinyint eligible_grades_mask
boolean active
}
CAMPAIGNS {
bigint id PK
bigint template_id FK
bigint brand_id
varchar policy_type
int discount_rate
int max_discount_amount
int valid_days
tinyint eligible_grades_mask
datetime open_at
datetime close_at
varchar status
}
COUPON_STOCKS {
bigint campaign_id PK
int total_quantity
int active_count
}
MEMBERS {
bigint id PK
varchar membership_grade FK
varbinary email_enc
char email_hash UK
varbinary phone_enc
char phone_hash
}
COUPONS {
bigint id PK
bigint campaign_id FK
bigint member_id FK
char code UK
varchar status
datetime issued_at
datetime expires_at
}
COUPON_HISTORIES {
bigint id PK
bigint coupon_id FK
varchar event_type
varchar from_status
varchar to_status
varchar reason
datetime created_at
}
COUPON_USAGES {
bigint id PK
bigint coupon_id FK
bigint order_id
int discount_amount
datetime used_at
datetime canceled_at
}
VERIFICATION_RUNS {
bigint id PK
datetime as_of
varchar dataset
int finding_count
}
VERIFICATION_FINDINGS {
bigint id PK
bigint run_id FK
varchar finding_type
bigint coupon_id
}

```

이 외에 `idempotency_records`(멱등키) · `campaign_stats` · `grade_stats` · `hourly_stats`(집계 3종)가 독립 테이블로 존재합니다.

## 테이블 정의

### brands · coupon_templates

| 테이블 | 컬럼 | 타입 | 비고 |

````
| brands | id | BIGINT | PK |
``
| name | VARCHAR(50) | 모카빈 · 씨네플러스 … 12개 |
``
| category | VARCHAR(20) | 카페 / 영화 / 외식. 통계 그룹핑용 |
``
``
| coupon_templates정책+반복 | id | BIGINT | PK |
``
| brand_id | BIGINT | FK |
``
| name | VARCHAR(100) | "신학기 요금제 할인" |
````````
| policy_type | VARCHAR(20) | PERCENT_CAPPED / FIXED_AMOUNT / DATA_GRANT |
``
| discount_rate | INT | 정률 전용. 20 = 20% |
``
| max_discount_amount | INT | 정률 상한. 20000 |
``
| discount_amount | INT | 정액 전용. 5000 |
``
| data_grant_mb | INT | 데이터 전용. 1024 |
``
| min_order_amount | INT | 최소 주문 금액 |
``
| valid_days | INT | 발급 후 유효 일수 |
``
| nth_week | TINYINT | 1~4. 매월 N번째 |
``````
| day_of_week | VARCHAR(3) | MON ~ SUN |
``
| start_time | TIME | 14:00 |
````
| eligible_grades_mask | TINYINT UNSIGNED | 비트마스크. VIP+GOLD = 12 |

정책 컬럼이 타입별로 나뉘어 대부분 `NULL`입니다. JSON 한 컬럼으로 묶을 수도 있지만, 컬럼으로 두면 통계 쿼리에서 바로 집계되고 타입 검증이 DB 레벨에서 걸립니다.

### campaigns · coupon_stocks

| 테이블 | 컬럼 | 타입 | 비고 |

``
``
| campaigns147행 | id | BIGINT | PK |
````
| template_id | BIGINT | FK. UNIQUE(template_id, open_at) |
``
| brand_id | BIGINT | 비정규화 — 통계 조인 1회 제거 |

| 정책 컬럼 일체 | — | 템플릿에서 복사한 스냅샷 |
``
| eligible_grades_mask | TINYINT UNSIGNED | 템플릿에서 복사 |
````
| open_at / close_at | DATETIME(6) | 캠페인 창 |
````````
| status | VARCHAR(12) | SCHEDULED / OPEN / CLOSED |
``
``
| coupon_stocks1:1 | campaign_id | BIGINT | PK |
``
| total_quantity | INT | 총 발급 가능 수량 |
``````
| active_count | INT | 현재 ISSUED + USED 개수. 취소·만료 시 감소 |

`campaigns`에 `total_quantity`를 두지 않습니다. 재고는 `coupon_stocks`에만 존재합니다.

### members · grades

| 테이블 | 컬럼 | 타입 | 비고 |

``
``
| members100만행 | id | BIGINT | PK. 내부 식별자라 암호화·마스킹 안 함 |
``````````
| membership_grade | VARCHAR(10) | VIP/GOLD/SILVER/WELCOME. 사람이 읽는 값 유지 |
``
| name_enc | VARBINARY(256) | AES |
``
| email_enc | VARBINARY(256) | AES |
``
| email_hash | CHAR(64) | HMAC-SHA256. UNIQUE는 여기에 |
````
| phone_enc / phone_hash | VARBINARY / CHAR(64) | 동일 방식 |
``
``
| grades4행 | code | VARCHAR(10) | PK |
``
| bit_value | TINYINT | WELCOME 1 · SILVER 2 · GOLD 4 · VIP 8 |

### coupons · coupon_histories · coupon_usages

| 테이블 | 컬럼 | 타입 | 비고 |

``
``
| coupons300만행 | id | BIGINT | PK |
``
| campaign_id | BIGINT | FK |
````
| member_id | BIGINT | FK. UNIQUE(campaign_id, member_id) |
``
| code | CHAR(16) | UNIQUE. 사용자 노출용 |
``````````
| status | VARCHAR(12) | ISSUED/USED/CANCELLED/EXPIRED |
``
| issued_at | DATETIME(6) | |
````
| expires_at | DATETIME(6) | issued_at + valid_days. 만료 판정의 유일한 기준 |
``
``
| coupon_historiesappend-only | id | BIGINT | PK |
``
| coupon_id | BIGINT | FK |
````````````
| event_type | VARCHAR(12) | ISSUE/USE/CANCEL_USE/CANCEL/EXPIRE |
````
| from_status / to_status | VARCHAR(12) | 전이 전 / 후 |
``
| reason | VARCHAR(100) | 배치 만료, 주문 취소 등 |
````
| coupon_usages | id | BIGINT | PK |
``
| coupon_id | BIGINT | FK. 한 쿠폰에 여러 행 가능 |
``
| order_id | BIGINT | 더미 정수. 외래키 없음 — 주문 도메인 미구현 |
``
| discount_amount | INT | 실제 할인 금액 |
``````
| used_at / canceled_at | DATETIME(6) | canceled_at IS NULL이 유효한 사용 |

coupon_usages 에 여러 행이 생기는 이유

더미데이터의 `USED 중 20%가 사용 → 사용취소 → 재사용` 이력이 여기 표현됩니다. 현재 유효한 사용은 `canceled_at IS NULL`인 행이고, 쿠폰당 최대 1개여야 합니다. 이것도 검증 대상입니다.

### 보조 테이블

| 테이블 | 핵심 컬럼 | 역할 |

````````
| idempotency_records | idem_key PK · request_hash · response_body | 상태 변경 멱등성. 동시 요청은 PK 제약으로 선착순 1건만. 24시간 후 정리 |
``````````````
| verification_runs | as_of · dataset · finding_count | 검증 실행 이력. dataset은 CLEAN/CORRUPT |
````````````
| verification_findings | run_id · finding_type · coupon_id | 검출 항목. 같은 as_of면 finding_type 집합이 동일해야 함 |
``
| campaign_stats | issued · used · cancelled · expired · sold_out_seconds | 브랜드 전환율 · 소진 랭킹 차트 |
``````
| grade_stats | campaign_id+grade PK · issued · used | 등급별 분포 차트 |
``````
| hourly_stats | day_of_week+hour PK · issued | 요일×시간 히트맵 |

집계 3종은 검증 배치가 300만 건을 전수 스캔하는 같은 패스에서 산출합니다. 추가 스캔 비용이 없습니다.

## 핵심 제약 — 불변식의 물리적 보장

```
-- 1인 1매의 최종 방어선
ALTER TABLE coupons
ADD CONSTRAINT uk_campaign_member UNIQUE (campaign_id, member_id);

-- 스케줄러 중복 실행 방어
ALTER TABLE campaigns
ADD CONSTRAINT uk_template_open UNIQUE (template_id, open_at);

-- PII 유니크는 해시 컬럼에 (암호문은 매번 달라짐)
ALTER TABLE members
ADD CONSTRAINT uk_email_hash UNIQUE (email_hash);
```

## 재고 카운터 — 이름이 곧 의미

issued_count 를 쓰지 마세요

```
coupon_stocks(campaign_id PK, total_quantity, active_count)

불변식: 잔여 = total_quantity − active_count
```

`issued_count`는 누적 발급 수로 읽히는데 우리 불변식은 그게 아닙니다. `active_count`는 현재 `ISSUED` + `USED` 개수이고 취소·만료 시 감소합니다. 이름을 잘못 잡으면 초과 발급 판정이 통째로 어긋납니다.

재고를 캠페인 행에서 떼어낸 이유 — 캠페인 행에 재고를 두면 조회와 수정이 같은 행에 몰려 v1의 잠금 경합이 커집니다. 별도 행이면 `SELECT ... FOR UPDATE`가 재고 행만 잠급니다.

## 등급 자격 — 비트마스크

선택 기준이 성능이 아닙니다. 등급 4개에 캠페인 147개면 어느 방식이든 차이가 무의미합니다. 기준은 발급 경로에 DB 접근이 없어야 한다는 원칙입니다.

| 방식 | 판정 |

| 별도 매핑 테이블 | 탈락 발급 경로에 조인 추가. 등급 검증을 JWT로 처리한 이유가 무너짐 |
``
| MySQL SET 타입 | 탈락 벤더 종속. repository 격리 원칙 위반 |
``
| 콤마 문자열 "VIP,GOLD" | 차선 파싱 코드가 여러 군데 생기고 공백·대소문자 실수가 남 |
``
| 비트마스크 | 채택 정수 하나. 애플리케이션에서는 EnumSet<Grade>로 타입 안전 |

```
public enum Grade { WELCOME(1), SILVER(2), GOLD(4), VIP(8) }

@Convert(converter = GradeSetConverter.class) // EnumSet ↔ TINYINT
private EnumSet<Grade> eligibleGrades;

// 발급 경로 — DB 접근 0
campaign.getEligibleGrades().contains(jwtGrade)
```

검증 배치만 4행짜리 `grades` 참조 테이블을 조인해 `(mask & bit_value) = 0` 으로 위반을 검출합니다. 조인 비용이 사실상 0입니다.

## 인덱스

검증 배치를 개발 중 수십 번 돌립니다. 없으면 매번 300만 건 풀스캔입니다.

```
CREATE INDEX idx_coupon_campaign_status ON coupons (campaign_id, status);
CREATE INDEX idx_coupon_status_expires ON coupons (status, expires_at);
CREATE INDEX idx_history_coupon ON coupon_histories (coupon_id, created_at);
CREATE INDEX idx_usage_coupon_active ON coupon_usages (coupon_id, canceled_at);
```

`uk_campaign_member`는 제약이자 인덱스라 1인 초과 발급 검출 쿼리를 그대로 커버합니다.

## 비정규화 3곳 — 그리고 금지된 1곳

| 위치 | 판정 | 이유 |

``
| campaigns.brand_id | 허용 | 통계가 브랜드별 집계를 자주 함. 조인 1회 제거 |
``
| campaigns의 정책 컬럼 | 허용 | 스냅샷. 템플릿을 바꿔도 이미 생성된 캠페인 조건이 변하면 안 됨 |
``
| campaigns.eligible_grades_mask | 허용 | 같은 이유 |
``
| total_quantity 양쪽 보유 | 금지 | 양쪽이 계속 변하는 값이라 어긋날 수 있음. 검증 대상만 늘어남 |

스냅샷이 중복과 다른 이유

3월 캠페인이 `20% 할인`으로 열렸는데 4월에 템플릿을 `15%`로 바꾸면 3월 쿠폰의 할인율이 소급 변경되고, `coupon_usages.discount_amount`와 어긋나 정합성 검증이 깨집니다. 스냅샷은 시점 고정이라 애초에 변하지 않으므로 검증 대상이 아닙니다.

## Redis에 사는 것

| 데이터 | 키 | DB에 두지 않는 이유 |

``
| 재고 카운터 | stock:{campaignId} | v2/v3 원자 연산 대상. DB는 사후 영속화 |
``
| 대기열 순번 | queue:{campaignId} | 초당 수천 건 삽입·삭제. DB로는 불가 |
``
| 순번 발급기 | queue:{id}:seq | 단조 증가 시퀀스 |
``
| 입장 토큰 | entry:{id}:{userId} | TTL 만료가 곧 슬롯 반납 |

`coupon_stocks.active_count`와 Redis 카운터가 어긋나는 것이 드리프트이고, 1초 주기로 대조합니다.

## 범위 밖

``
| 주문 도메인 | 만들지 않습니다. coupon_usages.order_id는 더미 정수이고 외래키를 걸지 않습니다. 쿠폰 사용 실적 기록이 목적이지 주문 관리가 아닙니다 |
``
| issue_requests 테이블 | 발급 요청 이력 적재는 초당 수만 건. 확장 항목 |

| 파티셔닝 · 읽기 복제본 | 300만 건 / DB 1대라 조건 미달 |
``
| version 컬럼 · 등급 쿼터 테이블 | 낙관적 락과 등급 쿼터는 범위 밖 |

2 vCPU 한계에서 무엇을 하고, 넘어서면 어디로 가는가. 측정 환경은 3층이고 확장 사다리에는 트리거가 붙어 있습니다.

## 측정 환경은 3층입니다

팀원 노트북에서 나온 수치를 비교표에 넣으면 v1/v2/v3 비교가 오염됩니다. 스펙이 제각각이기 때문입니다.

| 층 | 환경 | 목적 | 비교표에 쓰는가 |

| L1 개발 검증 | 각자 노트북 · Docker Compose | 정합성만 확인. 1,000 동시 | 절대 안 됨 |

| L2 기준 측정 | 팀 공용 지정 1대 · 2 vCPU / 8GB | 비교표를 여기서만 채움 | 여기 수치만 |

| L3 확장 측정 | 별도 서버 · 부하 생성기 분리 | 10만+ 트래픽 | 별도 표로 |

```
L2 기준 환경 (비교표의 유일한 출처)
Spring Boot 1대 2 vCPU / 8GB
Redis 1대 2 vCPU / 8GB
MySQL 1대 (컨테이너, 리소스 limit 고정)
k6 같은 머신 — 20,000 VU 까지는 감당 가능
```

L1에서 나온 숫자는 기록만 하고 비교하지 않습니다. M1 맥북과 Windows WSL2는 Docker I/O 성능이 크게 다릅니다. 정합성(초과 발급 0건)은 어디서 돌려도 같아야 하므로 L1은 정합성 전용입니다.

## 규모 감각 — 먼저 이걸 맞춰야 합니다

| 작업 | 2 vCPU 실측 대역 |

| 인메모리 응답만 | 5,000 ~ 10,000 RPS |

| Redis 왕복 1회 | 2,000 ~ 5,000 RPS |

| DB 쓰기 1회 | 300 ~ 1,000 RPS |

| v1 (비관적 락) | 수백 RPS — 커넥션 풀이 상한 |

처리 문제가 아니라 거절 문제입니다

20,000 TPS는 2 vCPU에서 처리할 수 있는 수치가 아닙니다. 수용 가능한 동시성을 넘으면 선택지는 셋뿐입니다.

지연
메모리 소비, 타임아웃 누적

거절
명확. 사용자는 즉시 안다

붕괴
전면 장애

세 번째만 피하면 됩니다. 이것이 admission control이 성능 최적화가 아니라 생존 전략인 이유입니다.

과제의 "20,000 동시"는 지속 TPS가 아닙니다. 재고 10,000장이 소진되면 끝나므로 실제 요구는 "20,000건을 몇 초 안에 소화하고 정확히 10,000건만 성공시키는가"입니다.

## 애플리케이션 레벨

`코어 × 2 + 1`은 CPU 바운드 공식입니다. 우리는 I/O 바운드라 다릅니다.

```
Little's Law: 스레드 수 = 목표 처리량 × 평균 응답시간

3,000 RPS × 20ms = 60 스레드
```

| 설정 | 출발값 | 근거 |

``
| server.tomcat.threads.max | 50 ~ 100 | Little's Law 기반. 기본 200은 2 vCPU에 과함 |
``
| server.tomcat.accept-count | 100 | 초과분은 여기서 대기 |
``
| hikari.maximum-pool-size | 10 ~ 20 | 2 vCPU DB면 이 이상 무의미 |
``
| lettuce.pool.max-active | 16 | Lettuce는 커넥션 공유 |

스레드 > 커넥션 격차를 없애지 마세요

```
Tomcat 스레드 100 > Hikari 커넥션 10
→ 90개 스레드가 커넥션을 기다리며 블록
→ 이것이 v1 병목의 형태
```

격차가 곧 측정 대상입니다. 튜닝으로 지우면 측정하려던 것이 사라집니다. v2/v3는 Redis 원자 연산이라 커넥션을 짧게 쓰고 놓으므로 같은 풀에서도 훨씬 많이 처리합니다 — 같은 설정에서 버전별로 다른 결과가 나오는 것이 비교의 핵심입니다.

### cgroup 함정

```
cpus: 2.0 → Runtime.availableProcessors() == 2 ✓
cpus: 0.5 → availableProcessors() == 1 ⚠ GC 스레드·ForkJoinPool 크기가 달라짐

JAVA_TOOL_OPTIONS: "-XX:ActiveProcessorCount=2 -XX:MaxRAMPercentage=75"
```

세 버전 측정 시 이 값을 반드시 동일하게 두세요. GC 스레드 수가 달라지면 비교가 오염됩니다.

### Virtual Thread — 버전마다 효과가 다릅니다

| 버전 | VT 효과 | 이유 |

``
| v1 | 거의 없음 | 커넥션 풀이 상한이라 VT를 써도 풀에서 막힘. synchronized·네이티브 락은 캐리어 스레드를 pin |

| v2 | 중간 | Redis 왕복 대기가 값싸짐. 다만 Redis 자체가 상한 |

| v3 | 가장 큼 | 응답을 빨리 돌려주고 대기가 많은 구조. VT의 전형적 이득 구간 |

VT는 처리량을 늘리지 않습니다. 대기를 값싸게 만들 뿐입니다. 병목이 커넥션 풀이면 VT를 켜도 그대로입니다. 6조합 측정은 예산 밖이라 1회 측정 후 발표에서 언급합니다.

## 트래픽 패턴

| 패턴 | 우리 상황 | 대응 |

| Hot key단일 키에 트래픽 집중 | Redis는 키 단위 단일 스레드. Cluster여도 한 슬롯 = 한 노드 | 확장 4단계 재고 키 샤딩. 다만 불변식이 샤드 합으로 확장되어 검증 재설계 필요. 10,000장엔 불필요 |

````
| Stampede캐시 만료 순간 원본 쇄도 | 오픈 순간이 정확히 이 지점 | 이미 대응 T-30초 워밍업이 답. 추가로 singleflight(Caffeine이 per-key 락 내장) + jitter 한 줄 |

| Null 조회없는 키가 캐시를 못 탐 | 캠페인 ID는 유한하고 목록 API가 있어 임의 ID 조회는 공격 상황 | Bloom filter는 오버 하려면 null object 30초 TTL 한 줄 |

## 확장 사다리

트리거 없는 확장 계획은 희망사항이고, 있으면 운영 계획입니다.

```
[지금 · 2 vCPU 싱글 노드]
스레드 50~100 / Hikari 10~20 / ActiveProcessorCount 고정
인덱스 4종 · T-30초 워밍업 · singleflight + jitter
admission control (거절 전략)
│
│ 트리거: 앱 CPU 지속 80%
▼
[1단계 · 수직 확장]
vCPU 증설 → 스레드·커넥션 재산정
가장 싸고 확실. 여기서 오래 버팁니다
│
│ 트리거: 수직 한계 도달
▼
[2단계 · 수평 확장]
in-flight 카운터 전역화 → HPA + LB
Redis가 다음 상한이 됨
│
│ 트리거: Redis CPU 70% · 캠페인 간 지연 상관
▼
[3단계 · 격리]
HOT/NORMAL 등급 + 워커 풀 분리 + HOT 전용 Redis
대기열 서버 분리도 이 단계
│
│ 트리거: 단일 캠페인 100만 TPS · 단일 키 슬롯 포화
▼
[4단계 · 샤딩]
재고 키 샤딩 → 불변식이 샤드 합으로 확장
```

큐를 나눠도 컨슈머를 공유하면 격리되지 않습니다. 자원을 나눠야 격리입니다. (3단계의 핵심)

## 수평 확장이 막히는 진짜 이유

```
앱을 2대로 늘려도

① in-flight 카운터가 인스턴스 로컬 → 각자 절반만 보고 대기열 판단
② Redis 여전히 1대 → 병목이 앱에서 Redis로 이동할 뿐
③ DB 여전히 1대 → v1은 아예 개선 없음
```

①이 우리 설계에 직접 걸립니다. 진입 판단이 인스턴스 로컬 값을 쓰기 때문에, 2대로 늘리는 순간 각 인스턴스가 전체 부하의 절반만 보고 판단하게 됩니다.

## 대기열 서버 분리 — 싱글 노드에서는 통합

```
서버를 2대로 나누면
① 리소스 총량이 늘어 v1/v2/v3 비교가 불공정해짐
② 2 vCPU를 1+1로 쪼개면 각각이 더 약해짐
③ 네트워크 홉 추가 + SPOF 추가
```

인터페이스는 분리 가능한 모양(`CapacityProbe`)으로 두되 같은 프로세스에서 호출합니다. 3단계에서 서버를 분리할 때 옮기기만 하면 됩니다.

이 구조는 credit 기반 backpressure입니다. TCP receive window, Reactive Streams `request(n)`, gRPC flow control과 같은 패턴입니다. 현재는 고정값(초당 500)을 반환하고, 실시간 여력 산출은 확장 1~2단계 항목입니다.

## L3 — 10만 트래픽 측정 환경

L2(2 vCPU)에서 10만은 불가능합니다. 별도 서버로 옮길 때 새로 나타나는 병목이 있습니다.

### 부하 생성기를 반드시 분리합니다

```
❌ 같은 머신: k6 20,000 VU 가 CPU 를 먹어 서버와 경합
→ 측정한 것이 서버 성능인지 k6 성능인지 알 수 없음

✅ 분리: [k6 노드] ──네트워크──> [앱 노드] ── [Redis] [MySQL]
```

L2에서도 이미 경계선입니다. 20,000까지는 감당되지만 10만은 반드시 분리해야 하고, k6 단일 인스턴스로도 부족해 `--execution-segment`로 분산 실행하거나 부하 노드를 여러 대 둡니다.

### OS 레벨에서 먼저 막힙니다

애플리케이션 튜닝 전에 커널 한계가 먼저 옵니다.

| 한계 | 증상 | 설정 |

``````
| 파일 디스크립터 | Too many open files | ulimit -n 1048576 · Compose ulimits.nofile |
``
| ephemeral port 고갈 | 부하 생성기 쪽 연결 실패 | ip_local_port_range = 1024 65535 |
``
| TIME_WAIT 누적 | 소켓이 안 돌아옴 | tcp_tw_reuse = 1 |
``
| SYN 백로그 | 연결 드롭 | somaxconn = 65535 |
````
| conntrack 테이블 | 패킷 드롭 (Docker 브리지) | nf_conntrack_max 증설 또는 network_mode: host |

`somaxconn`이 Tomcat `accept-count`보다 작으면 애플리케이션 설정이 무의미해집니다. 커널이 먼저 자릅니다.

### 네트워크가 새 변수로 들어옵니다

```
10만 req/s × 1KB 응답 = 100 MB/s = 800 Mbps
```

1 Gbps NIC이면 대역폭이 먼저 포화됩니다. L2에서는 같은 머신이라 안 보이던 문제입니다.

### 스레드·커넥션 재산정

```
L2 값(스레드 50~100 / Hikari 10~20)은 2 vCPU 기준. 그대로 옮기면 안 됨

Little's Law 재적용
8 vCPU · 목표 30,000 RPS · 응답 20ms
→ 스레드 ≈ 600, Hikari ≈ 40~60
```

vCPU를 4배로 올려도 처리량이 4배가 되지 않습니다. 어디서 선형성이 깨지는지가 1단계 → 2단계 전환의 트리거입니다.

L3 결과는 별도 표로 분리합니다. 환경이 다르면 같은 표에 넣지 않는 것이 원칙입니다 — L2 비교표는 "어느 구조가 어떤 병목을 갖는가", L3 확장 검증표는 "수직 확장이 어디까지 선형인가"로 목적이 다릅니다.

## 범위 판정

| 구성 | 판정 | 근거 |

| v1/v2/v3 · 검증 체계 · 스레드/커넥션 튜닝 · 인덱스 4종 | 필수 | 없으면 과제 미달이거나 측정이 왜곡됨 |
``
| /entry 분리 · CB 3분리 · singleflight+jitter | 정당 | 각각 명확한 이유가 있고 비용이 작음 |

| ADAPTIVE 모드 · 패널 24종 | 경계 | OFF/ALWAYS만으로도 비교 성립. 통계 5종은 줄여도 됨 |

| 대기열 서버 분리 · Bloom filter · 파티셔닝 · 재고 샤딩 · VT 6조합 | 범위 밖 | 데이터 규모·노드 수가 조건을 못 만족하거나 예산 초과 |

선착순은 구조적으로 어뷰징 표적입니다. 실제로 비어 있던 구멍 4건과 3주 안에 막을 것.

## 위협 모델

과제가 명시적으로 요구하는 보안은 개인정보 마스킹 하나입니다. 그런데 선착순 이벤트는 구조적으로 어뷰징 표적이라 최소 방어선이 없으면 시연 중에도 무너집니다.

| 자산 | 위협 | 손실 |

| 재고 | 어뷰징 발급 | 정상 사용자가 못 받음. 과제 핵심 조건이 무너짐 |

| 회원 PII | 이름·연락처 유출 | 과제 명시 요건 위반 → 직접 감점 |

| 운영 제어 | 관리 API 무단 사용 | 캠페인 조작, 검증 결과 위조 |

| 가용성 | 대기열 우회·폭주 | 시연 중 서버 다운 |

공격자 가정 — 부하 테스트 클라이언트를 조작할 수 있는 사람. 즉 토큰을 가진 내부 사용자가 기본 위협 모델입니다. 사전 발급 토큰만 유효하므로 임의 계정 생성은 불가능합니다.

## 🔴 설계에 비어 있던 구멍 4건

아래 4건은 새로 발견한 구멍이고, 기존 PII 요구(마스킹·암호화)와 actuator 노출 최소화를 합치면 필수 작업은 6건입니다.

① 관리 API · Actuator 무인증 노출

```
POST /api/v1/admin/campaigns → 누구나 캠페인 생성
POST /api/v1/admin/verify → 누구나 검증 배치 실행
GET /actuator/admission-capacity → 시스템 여력 노출
GET /actuator/env → 환경변수(= AES 키!) 노출
```

`/actuator/env`가 특히 위험합니다. AES·HMAC 키를 Compose 환경변수로 주입하기로 했으므로 이 엔드포인트가 열려 있으면 키가 그대로 나갑니다.

```
# ① 관리 포트 분리 — Compose 에서 외부 노출 안 함
management.server.port: 9090
management.endpoints.web.exposure.include: health,metrics,admission-capacity
management.endpoints.web.exposure.exclude: env,configprops,beans,heapdump

// ② /admin/* 는 ADMIN 역할 클레임 요구
.requestMatchers("/api/v1/admin/**").hasRole("ADMIN")
```

`exclude`를 명시하세요. `include`만 쓰면 나중에 누가 `*`로 바꿀 때 `env`가 함께 열립니다.

② Entry-Token 재사용

현재는 TTL 180초만 있고 한 토큰으로 `/issue`를 여러 번 호출하는 것을 막지 않습니다.

```
UNIQUE(campaign_id, member_id) 가 중복 발급은 막지만
토큰 하나로 재고 카운터를 반복 두드리는 것은 못 막는다
→ 소진 상태에서 재시도 폭주
```

대응 — 1회성 토큰. `GETDEL`(Redis 6.2+)로 조회와 삭제를 원자적으로 묶으면 동시 요청에서도 하나만 통과합니다. `nil`이면 `403 ENTRY_TOKEN_EXPIRED`.

원자성을 검증하는 테스트가 필요합니다. "동시 요청에서도 하나만 통과"가 `GETDEL`을 택한 근거이므로, 확인하지 않으면 근거가 비어 있는 셈입니다 — 실행 계획 탭 동시성 시나리오 13.

③ alg: none 우회

JWT 검증 시 토큰이 주장하는 알고리즘을 그대로 믿으면 서명 검증이 무력화됩니다.

```
공격: { "alg": "none" } + 서명 없음 → 라이브러리가 검증을 건너뜀

대응: .sig().add(Jwts.SIG.HS256) // 서버가 알고리즘 강제
```

④ 시크릿 커밋 위험

D1 최우선으로 `.gitignore`(`.env`, `*.jks`, `*.p12`). 커밋 이력에 한 번이라도 들어가면 히스토리 삭제만으로 부족하고 키를 교체해야 합니다.

## 어뷰징 방어

| 위협 | 방어 | 상태 |

``
| 한 유저의 폭주 요청 | UNIQUE(campaign_id, member_id) + 1회성 토큰 | 설계됨 |

| 다계정 발급 | 사전 발급 토큰만 유효 → 임의 계정 생성 불가 | 구조적 차단 |
````
| 대기열 우회 | Entry-Token 없는 /issue 차단 | 설계됨 |
``
| 오픈 전 선점 | 409 NOT_OPENED + 사전 진입 금지 | 설계됨 |

| IP 단위 폭주 | Bucket4j rate limit | 프로파일 분리 |

| 봇·매크로 | CAPTCHA, 디바이스 핑거프린팅 | 범위 밖 |

IP rate limit은 부하 테스트에서 끕니다

k6가 단일 호스트에서 20,000 VU를 쏘므로 켜두면 테스트 자체가 막힙니다. `application-loadtest.yml`로 분리합니다.

발표에서 밝히세요. 감추면 질문받았을 때 곤란해지고, 먼저 말하면 정직성이 됩니다 — "부하 테스트를 위해 껐고 실서비스에서는 켭니다."

## 개인정보 보호

| 지점 | 방어 |

``
| 저장 | AES-256-GCM. @Convert AttributeConverter |

| 검색 | HMAC-SHA256 해시 컬럼에 인덱스·유니크 |
``
| API 응답 | @Mask + Jackson Serializer |

| 로그 | Logback Converter 패턴 마스킹 |
``
| 검증 리포트 | 집계값만. member_id는 남기되 이름·연락처 금지 |
``
| 에러 응답 | 스택트레이스·SQL 원문 금지. code와 안내 문구만 |

가장 새기 쉬운 두 곳

```
// ❌ 예외 메시지에 파라미터가 그대로
"Cannot issue coupon for member 홍길동 (010-1234-5678)"

// ❌ 엔티티를 통째로 찍으면 @ToString 이 PII 를 뱉음
log.info("issued: {}", member);

// ✅ 식별자만 + @ToString(exclude = {"nameEnc","emailEnc"})
log.info("issued: memberId={}, campaignId={}", member.getId(), campaignId);
```

`server.error.include-stacktrace: never` 와 `@ControllerAdvice`로 응답 본문을 통제하세요.

AES 키 관리의 한계 — 이걸 아는 것이 발표 소재

환경변수로 주입하면 `/proc/<pid>/environ`에 평문으로 존재합니다. 컨테이너에 들어올 수 있으면 읽을 수 있습니다. 3주 과제에서는 환경변수가 현실적 상한이고, 실서비스는 KMS·Vault로 런타임 주입 + 로테이션이 필요합니다.

## 주입 · 입력 검증

| 항목 | 상태 | 비고 |

| SQL Injection | 구조적 차단 | JPA·PreparedStatement. 단, 동적 정렬·페이징은 화이트리스트 |
``````
| Redis Lua Injection | 주의 | EVAL에 사용자 입력 문자열 결합 금지. KEYS·ARGV로만 |

| 대용량 페이로드 | 설정 | 헤더·멀티파트 크기 제한 |
``
| 의존성 취약점 | D13 | dependencyCheckAnalyze 1회 |

```
-- ❌ 키 이름에 사용자 입력이 섞이면 다른 캠페인 재고를 건드릴 수 있음
redis.call('DECR', 'stock:' .. userInput)

-- ✅ KEYS 로 전달, 애플리케이션에서 숫자 파싱된 campaignId 만
redis.call('DECR', KEYS[1])
```

## 3주 범위 판정

| 항목 | 판정 |

``````
| 관리 포트 분리 + role: ADMIN · 1회성 토큰 · alg 고정 · .gitignore · actuator 최소화 · PII 3계층 · 에러·로그 마스킹 | 필수 6건 합쳐서 약 0.5일. 대부분 설정과 어노테이션 |

| IP rate limit(프로파일 분리) · 의존성 스캔 1회 | 채택 |

| CAPTCHA · 핑거프린팅 · KMS/Vault · WAF · 감사 로그 | 범위 밖 |

## 발표에서 말할 것

선착순 이벤트는 구조적으로 어뷰징 표적이라 세 겹으로 막았습니다. DB 유니크 제약이 1인 1매를 물리적으로 보장하고, 1회성 입장 토큰이 재고 카운터 반복 호출을 막고, 관리 포트 분리로 운영 API를 외부에서 못 건드리게 했습니다.

다만 AES 키를 환경변수로 주입하는 것이 저희 범위의 상한입니다. 컨테이너에 들어올 수 있으면 읽을 수 있어서, 실서비스라면 KMS로 런타임 주입하고 로테이션해야 합니다.

그리고 부하 테스트를 위해 IP rate limit을 프로파일로 껐습니다. k6가 단일 호스트에서 2만 VU를 쏘기 때문인데, 기본 프로파일에서는 켜져 있습니다.

보안은 "했다"보다 "어디까지가 우리 범위인지 안다"가 점수입니다. 마지막 문단을 먼저 밝히는 것이 정직성이 됩니다.

PRD v4.15 · 미결 사항 0건

유일한 외부 입력은 프로젝트 착수일이며, 실행 계획 탭의 `D1`에 대입하면 전 일정이 확정됩니다.
