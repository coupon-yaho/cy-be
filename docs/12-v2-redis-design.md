# 12 · v2 Redis 발급 설계

v1(`SELECT … FOR UPDATE`)에서 v2(Redis Lua 원자 카운터)로. 발급 경로와, 같은 Redis 자료구조를 재료로 쓰는 조회 경로를 함께 설계한다.

**아키텍처만 검토한다면** — §0 결정 · §4 발급 경로 · §5 재고 복원 · §6 장애·재기동 · §7 조회 · §9 정합성,
그리고 아키텍처 제약으로 작동하는 §3.3(hot key 명령 규칙). 나머지는 측정 전제와 v1 기준선이라 건너뛰어도 된다.
검토 의견은 **절 번호로 달아라** — 이 문서가 원본이므로 번호가 그대로 찾아진다.

---

## 0. 결정

| # | 항목 | 결정 |
|---|---|---|
| D1 | 부하 | 스파이크 — 2만 요청이 1~3초 내 전량 |
| D2 | 영속화 | Redis 선점 → 동기 DB INSERT → 실패 시 보상 롤백 |
| D3 | 재고 복원 | 취소·사용취소·만료도 Redis 동시 갱신. 순서는 DB→Redis(발급과 반대) |
| D4 | Redis 가용성 | 당장 대응 없음. replica·Sentinel 없음, redisCB 열리면 503 |
| D5 | 조회 캐시 | 2계층 — L1 Caffeine(힙) + L2 Redis. SWR 미채택, stale-if-error 만 |
| D6 | 멱등 | Redis `HSETNX` 가 게이트만 대체. 레코드는 발급 TX 안에서 DONE. 값 전이는 **요청토큰 CAS**(§4.4), stale **자동 회수 없음**(§4.10) |
| D7 | 대기열 | v2 측정에서 OFF |
| D8 | hot key | 분할 안 함. 대신 O(N) 명령 차단 |
| D9 | `active_count` | 프로젝터 주기 갱신 + `severityForGap` 버전별 분기 |
| **D10** | **아키텍처 확정** | **구조 변경 없음.** 이 문서의 §4~§9 로 확정하고, 남은 조정은 설정값뿐이다(§0.1) |
| **D11** | 거절 정책 | **재고 잔여 429 없음.** Lua 를 통과한 요청은 전원 성공시킨다(§10.4) |
| **D12** | 응답 계약 | **201 Created 유지.** 202 접수 방식으로 가지 않는다 |

### 0.1 확정과 반려

검토에서 나온 구조 변경 제안을 **전부 반려한다.** 이 문서의 설계로 확정하고,
남은 작업은 **정합성 구현**(§4.4·§4.10·§5.2 등)과 **설정값 조정**뿐이다.

| 반려 항목 | 반려 시 남는 것 | 재검토 조건 |
|---|---|---|
| 인프로세스 마이크로배칭 | **동기 단건 커밋 유지.** 성공 경로 상한이 커넥션 풀 × 트랜잭션 시간으로 고정된다 | §10 측정이 목표 시간을 못 맞출 때. 그때도 먼저 조정할 것은 **인스턴스 수·도착 시간·회차 재고**다 |
| 배치 단위 `active_count` UPDATE | **프로젝터 유지**(D9). 절대값 쓰기라 중복 실행이 안전하다 | 마이크로배칭을 채택할 때만 의미가 있다 |
| 회원별 표시 인덱스 | **회차별 `HEXISTS` 유지.** 목록 1건이 Redis 명령 20여 개를 쓴다 | §7.6 예산을 **명령 수 기준**으로 다시 재고, 게이트가 밀릴 때 |
| 조회 캐시 Redis 물리 분리 | **단일 Redis 유지.** 조회와 발급이 같은 실행 스레드를 공유한다 | 혼합 부하에서 조회가 게이트 지연을 만들 때 |
| generation 기반 재구성 | **§6.2 절차 유지.** 재구성 중 그 회차는 전면 503 이다 | 워밍업 무중단이 필요해질 때 |
| L1 TTL 경계 정렬 · stale 보관 | **§7.2·§7.3 유지.** L1 고정 10초, stale-if-error 는 정책만 | 거절 코드 분포가 해석 불가할 때 |
| `issued` 키 수명 정책 | **정하지 않는다.** TTL 없음·`noeviction` 이므로 회차가 쌓이면 메모리가 단조 증가한다 | 캠페인을 반복 운영할 때. **지금은 회차 수가 유한하다는 전제** |
| 재구성 시 멱등키 복원 | **`__rebuilt__` 유지.** 재구성 이후 replay 응답 재사용이 불가능하다 | 위와 같음 |
| reservation + 202 Accepted | **동기 201 유지**(D12) | 위 전부를 조정하고도 목표를 못 맞출 때 |
| 재고 잔여 429 | **거절하지 않는다**(D11) | 없음. 선착순 정의가 바뀌므로 제품 결정이 선행해야 한다 |
| DB 퍼밋을 Lua 보다 먼저 | 순서 유지 — Lua 가 먼저다 | 없음. 거절 요청까지 DB 용량 경쟁에 참여시킨다 |
| 발급 경로 재시도 | 재시도 없음 | 없음. 타임아웃 뒤 재시도는 선점이 이미 성공했을 수 있다 |

**반려는 "틀렸다"가 아니라 "지금 하지 않는다"이다.** 재검토 조건을 함께 적어 둔 이유가 그것이고,
조건이 충족되기 전에는 다시 꺼내지 않는다.

---

## 1. 전제

**L2 — 물리 PC 3대, 서버 간 유선 LAN.**

| | 자원 | 설정 |
|---|---|---|
| api | 6코어 / 16GB | Tomcat worker 60 · Hikari pool 12 |
| MySQL | 6코어 / 16GB | `max_connections` 500 · buffer pool 5GiB · redo 1GiB · `io_capacity` 10000 · SATA SSD · `flush_log_at_trx_commit=1` · `sync_binlog=1` |
| Redis | 6코어 / 16GB | AOF everysec · `noeviction` |

부하 생성기는 Locust, 노트북에서 **무선**으로 api 에 붙는다(이 구간만 무선).

```
회차당 재고 10,000 · 요청 20,000+ · 도착 1~3초
발급 p99 ≤ 500ms · 조회 p99 ≤ 100ms
초과 발급 0건 · 1인 1매(평생, 취소·만료 후에도 재발급 불가)
```

**최종 방어선은 DB다.** `uk_coupon_member UNIQUE (coupon_id, member_id)` 가 살아 있는 한 v2 에 버그가 있어도 1인 2매는 물리적으로 불가능하다. Redis 는 그 앞의 빠른 게이트지 정본이 아니다 — §6 의 근거.

**api 는 1대다.** 조회 캐시의 single flight 락을 로컬에 두는 근거(§7.2).

---

## 2. v1 기준선 — V1-2

**v2 는 `V1-2` 를 기반으로 한다.** v1 은 두 변종이 있다.

| 브랜치 | 발급 경로 | 비고 |
|---|---|---|
| `V1-1` | `FOR UPDATE` 로 먼저 잠그고 그 안에서 INSERT | `main` 과 동일한 코드에 이름표를 붙인 것 |
| **`V1-2`** | **`FOR UPDATE` 제거. 조건부 원자 UPDATE 하나** | `perf: V1-2 쿠폰 재고 차감 원자화` (1b5c7fa5) |

V1-2 는 `main` 기반이라 `feature/CY-5` 에 없다. v2 착수 전에 가져와야 한다.

### 2.1 V1-2 발급 경로

```
[TX1] 멱등 선점 (REQUIRES_NEW)                        커밋 1회
      정책 검증 (readOnly)                             fsync 없음
[TX2] issue + 멱등 DONE                                커밋 1회
        findById
        → issuances INSERT
        → issuance_histories INSERT
        → occupyOne  UPDATE … WHERE active_count < total_quantity
```

발급 1건 = **쓰기 커밋 2회 = fsync 4회**.

V1-2 가 바꾼 것 셋 — ① `lockForUpdate` 를 발급 경로에서 제거 ② `occupyAfterLock` → `occupyOne` 개명 ③ `NOT_FOUND` 를 실제로 반환(`existsById` 로 매진과 구분).

### 2.2 남은 병목

- **직렬화는 남는다** — `occupyOne` 이 회차 행에 X 락을 잡고 커밋까지 유지한다. `FOR UPDATE` 를 없애 락 보유 구간이 `UPDATE ~ COMMIT` 으로 줄었을 뿐, 회차당 직렬화 자체는 그대로다. 그 구간의 대부분이 커밋 fsync 이므로 천장은 대략 `1 / fsync`.
- **거절도 DB 를 탄다** — 재고 1만 / 요청 2만이면 절반이 매진인데, 매진 판정(`occupyOne`)이 **맨 마지막**이라 그 1만 건이 멱등 INSERT 를 커밋하고 `issuances` + `issuance_histories` 두 건을 INSERT 한 뒤에야 롤백된다.
- **잠금 순서 통일이 깨졌다** — §8 참조.

v2 의 절반은 원자 카운터, 나머지 절반은 **거절을 DB 에 닿기 전에 끝내는 것**이다.

---

## 3. 상한

### 3.1 Redis 는 병목이 아니다 (실측)

발급 Lua 의 명령 6개는 전부 O(1). Redis 7.4 컨테이너 실측:

| | 값 |
|---|---|
| 단일 키 처리량 | **112,941 rps** (필요 20,000 대비 5.6배) |
| 지연 | avg 0.42 · p50 0.38 · p95 0.70 · **p99 1.40** · max 10.32 ms |

`max 10.32ms` 때문에 Lua 호출 타임아웃을 100ms 로 둔다(§6.3). L2 는 Redis 전용 머신 + 유선이라 처리량은 이보다 높고 지연에 RTT 0.1~0.3ms 가 더해진다.

### 3.2 MySQL 이 천장이다

buffer pool 5GiB 가 working set(`issuances` 1.0~1.2GB + `members` 0.5GB + `histories` 0.5~1.0GB ≈ 2.5~3.5GB)을 덮는다. INSERT 가 랜덤 I/O 를 안 내고, 커밋 때 치는 것은 redo·binlog fsync 두 번 — 그것도 순차 쓰기.

```
SQL 실행 (buffer pool hit)      ~0.2 ms
커밋 fsync ×2 (순차, SATA SSD)  ~0.3~0.6 ms
네트워크 (유선, 2~3 RTT)        ~0.5 ms
────────────────────────────────────────
트랜잭션                        ~1.0~1.3 ms
```

`상한 = pool 12 ÷ 트랜잭션` → 이론 10,000 TPS, 경합 감안 **4,000~8,000 TPS**. 성공 1만 건 소화 **1.25~2.5초**. 도착 3초면 따라가고 1초면 큐가 쌓여 p99 가 발산한다.

**왜 천장인가.** 커밋 비용은 행이 아니라 트랜잭션에 붙는다. 발급 1건 = 트랜잭션 1개 = fsync 2회이고, 행을 몇 개 넣든 마찬가지다. CPU 도 커넥션도 남는데 여기서 막힌다.

실무가 이 벽을 넘는 방법은 **쓰기를 모아 한 트랜잭션으로 커밋하는 것** 하나뿐이다(100건 묶으면 1건당 0.02회, 50배). 그게 **v3(Redis + Kafka)** 다. v2 가 3,300 TPS 를 못 내는 것은 설계 실패가 아니라 개별 커밋 구조의 물리적 상한이고, v3 는 그 상한을 우회하려고 존재한다.

**선점과 영속을 분리해 둔 것이 여기서 값을 한다.** 선착순 순서는 Redis 게이트에서 확정되므로 DB 영속 순서는 결과에 영향이 없다. v1 처럼 DB 락으로 순서를 정했다면 v3 의 배칭 자체가 불가능했다.

측정 지표를 **성공 p99 와 거절 p99 로 분리**한다(§10). 섞은 단일 p99 는 매진 1만 건이 분포를 끌어내린다.

### 3.3 hot key — 무엇이 남는가

**"Redis 가 1대라 hot key 문제가 없다"는 틀렸다.** 처리량 축 하나만 무의미해지고 나머지는 남는다.

| 실측 | 값 |
|---|---|
| 단일 키 `c=48` | 112,941 rps · p50 0.383ms |
| 8분할 프로세스 8×`c=6` | 78,300 rps · p50 0.52ms |

`io_threads_active:0` — 명령 실행이 실제로 단일 스레드라 분할이 **31% 손해**다.

**진짜 남는 문제는 head-of-line blocking.** 느린 명령 하나가 뒤의 모든 요청을 세운다.

| 명령 (175k field Hash) | 소요 | 다른 클라이언트 PING |
|---|---|---|
| `HGETALL` | **26.2 ms** | 0.26ms → **19ms** |
| `DEL` | 11.6 ms | 〃 |
| `UNLINK` | < 1 ms | 영향 없음 |
| `DEL` (10k = 실제 규모) | 0.38 ms | 영향 없음 |

`HGETALL` 한 번에 20,000 rps 구간이면 520건이 적체된다. 현재 규모(회차당 1만)는 안전하므로 **분할 대신 O(N) 을 몰아낸다.**

| 규칙 | 이유 |
|---|---|
| 발급 Lua 는 O(1) 만. 루프 금지 | 현 6개 전부 O(1) |
| 조회는 `HEXISTS`·`HLEN` 만. `HGETALL`·`HKEYS`·`SMEMBERS`·`KEYS` 금지 | 26ms 블로킹 |
| 삭제는 `DEL` 대신 **`UNLINK`** | 11.6ms → 1ms 미만 |
| 재구성 `HSET` 은 1,000건 배치 | Lua 루프로 1만 건은 그 시간 전체가 블로킹 |
| `slowlog-log-slower-than` **1000µs** | 1ms 초과가 사고의 전조. 컨테이너 기본 10ms 는 못 잡는다 |
| 캐시 값을 크게 만들지 않는다 | 같은 스레드를 발급과 공유 |

분할이 의미를 갖는 축은 처리량이 아니라 이 블로킹 시간이다(175k→11.6ms, 10k→0.38ms 로 선형). **회차당 재고가 18,000~34,000 으로 커지면 재검토한다.**

분할로도 `UNLINK` 로도 안 풀리는 것 — 발급과 조회가 같은 단일 스레드를 공유하고(논리 DB 를 나눠도 스레드는 하나), AOF rewrite fork·eviction 이 그 스레드를 멈추며, Lettuce 커넥션 멀티플렉싱에서 같은 blocking 이 클라이언트 쪽에도 재현된다.

---

## 4. v2 발급 경로

### 4.1 키

| 키 | 타입 | 내용 | 크기(재고 1만) |
|---|---|---|---|
| `cy:v2:stock:{r}` | String | 잔여 재고 = 활성 기준 | 수십 B |
| `cy:v2:issued_ever:{r}` | String | 누적 발급 수. 취소로 줄지 않음 | 수십 B |
| `cy:v2:issued:{r}` | Hash | field=memberId, value=`"<상태>\|<선점시각>\|<요청토큰>\|<멱등키>"` | ~1MB |
| `cy:v2:meta:{r}` | Hash | status·openAt·closeAt·gradeMask·totalQuantity | 수백 B |

동시 OPEN 12회차면 약 12MB. 해시태그 `{r}` 로 묶어 Cluster 로 가도 같은 슬롯에 떨어진다.

- **`issued_ever` 를 따로 두는 이유** — 취소·만료가 `stock` 을 되돌리므로 `total − stock` 은 **활성 수**이지 **누적 수**가 아니다. `LUA_GAP`·`PERSIST_GAP` 두 축이 누적을 요구한다(§9.2).
- **`issued` Hash 가 멱등 게이트를 겸한다**(D6). `HSETNX` 가 `tryStart` 의 동시 중복 차단을 대신하고, 상태 `P → D` 가 "선점됐지만 아직 영속 전"을 표현한다. v2 에서는 간격이 밀리초지만 **v3 에서는 필수**다.
- **값은 네 필드 고정이다** — `<상태>|<선점시각>|<요청토큰>|<멱등키>`. 상태는 `P`(PENDING)·`D`(DONE) 둘뿐이고 **다른 값을 추가하지 않는다**(§4.4). 멱등키를 맨 뒤에 둔 이유는 키 안에 `|` 가 들어와도 앞 세 필드가 흔들리지 않게 하기 위해서다. 앞 세 필드는 `|` 를 포함하지 않는다.
- **`meta` 를 올리는 이유** — 정책 검증(오픈·마감·등급)에 DB 를 읽지 않기 위해서. 이게 없으면 거절도 DB 를 한 번 읽는다. **`meta` 키의 존재 자체가 게이트**다(§6.2).

### 4.2 발급 선점 스크립트

```lua
-- KEYS[1]=stock KEYS[2]=issued KEYS[3]=meta KEYS[4]=issued_ever
-- ARGV[1]=memberId ARGV[2]=nowMillis ARGV[3]=gradeBit ARGV[4]=idempotencyKey ARGV[5]=requestToken

local meta = redis.call('HMGET', KEYS[3], 'status','openAt','closeAt','gradeMask')
if not meta[1] then return {-9} end                        -- NOT_READY: 재구성 중
local now = tonumber(ARGV[2])
if meta[1] ~= 'OPEN' or now >= tonumber(meta[3]) then return {-1} end   -- CAMPAIGN_CLOSED
if now < tonumber(meta[2]) then return {-2} end                          -- NOT_OPENED
if bit.band(tonumber(meta[4]), tonumber(ARGV[3])) == 0 then return {-3} end -- GRADE

-- 선점과 중복 판정을 한 명령으로. 재고 확인보다 먼저 한다.
local claimed = redis.call('HSETNX', KEYS[2], ARGV[1],
        'P|' .. ARGV[2] .. '|' .. ARGV[5] .. '|' .. ARGV[4])
if claimed == 0 then
    local stored = redis.call('HGET', KEYS[2], ARGV[1])
    local st, ms, tk, key = string.match(stored, '^(%a)|(%d+)|([^|]+)|(.*)$')
    if st == nil then return {-8} end                       -- 값 형식 파손. 경보
    if key ~= ARGV[4] then                                 -- 길이까지 정확히 같을 때만 같은 키
        return {-4}                                        -- DUP_PER_MEMBER
    end
    -- 같은 멱등키의 재시도. PENDING 과 DONE 을 반드시 가른다
    if st == 'D' then
        return {-6}                                        -- REPLAY_DONE
    end
    return {-7}                                            -- REPLAY_PENDING
end

local left = tonumber(redis.call('GET', KEYS[1]))
if left == nil or left <= 0 then
    redis.call('HDEL', KEYS[2], ARGV[1])                   -- 방금 잡은 선점만 되돌린다
    return {-5}                                            -- SOLD_OUT
end

redis.call('DECR', KEYS[1])
redis.call('INCR', KEYS[4])
return {0, left - 1}
```

**재고 확인·차감·중복 판정이 다른 요청 사이에 끼어들 수 없다.** v1 의 행 잠금이 하던 역할을 락 없이 대신한다.

순서에 의도가 있다 — **중복 선점을 재고 확인보다 먼저** 한다. 반대로 두면 "재고를 깎았는데 중복이라 되돌린다"가 되어 그 찰나에 다른 요청이 매진으로 거절될 수 있다. `SOLD_OUT` 일 때만 방금 자기가 만든 필드를 `HDEL` 하며, `HSETNX` 반환값 1이 그것을 보장한다.

`DECR stock`·`INCR issued_ever`·`HSETNX issued` 세 쓰기가 **반드시 같은 스크립트 안**에 있어야 한다. 하나라도 밖으로 나가면 `LUA_GAP` 이 즉시 CRITICAL 이 된다.

**멱등키는 접두가 아니라 전체가 일치해야 한다.** 멱등키는 클라이언트가 정하므로 `abc` 와 `abcdef` 같은 접두 충돌을 유도할 수 있고, 접두 비교로 두면 §4.4 보상이 **남의 선점을 되돌린다** — 초과 발급 방향이다. 그래서 값의 마지막 필드를 통째로 비교하고, 완료 여부도 `|DONE|` 문자열 탐색이 아니라 첫 필드가 `D` 인지로 판정한다. `-8` 은 값 형식이 깨진 경우이며 정상 운영에서 0이어야 한다.

**`요청토큰` 은 "이 선점이 누구 것인가"를 가리킨다.** 같은 멱등키로 두 번 들어온 요청도 토큰은 서로 다르다. 완료 승격과 보상(§4.4)이 **자기 토큰일 때만** 값을 건드리게 하는 근거가 이 필드다. 전역 유일성은 필요 없고 같은 field 안에서만 겹치지 않으면 되므로 `<인스턴스ID>-<스레드ID>-<카운터>` 로 충분하다. `|` 를 포함해서는 안 된다.

### 4.3 요청 시퀀스

```
POST /coupon-rounds/{r}/issue
 ├ ① 발급 선점 Lua ── Redis 왕복 1회
 │    -1 -2 -3 -4 -5 → 종료. DB 접촉 0회. 4xx
 │    -8            → 500 + 경보 (값 형식 파손. 정상 운영에서 0)
 │    -9            → 503 + Retry-After (재구성 중)
 │    -6 REPLAY_DONE    → DB 조회 후 저장된 응답 반환
 │    -7 REPLAY_PENDING → 409 + Retry-After. **폴링하지 않는다** (§4.8)
 │    0             → 아래로
 ├ ② [단일 TX] issuances + histories + idempotency(DONE)   커밋 1회
 │    성공 → **완료 CAS**(§4.4) 로 DONE 승격 → 201 + 잔여재고
 └ ③ 예외 → **커밋 여부 확인 후** 보상 (§4.9). 불확실하면 보상하지 않는다
```

거절 경로의 왕복은 Redis 1회뿐. 매진 1만 건이 DB 커넥션도 fsync 도 건드리지 않는다.

**`-4` 와 `-6` 을 같은 응답으로 만들면 멱등이 깨진다.** "중복"으로 뭉뚱그리면 안 되는 이유가 여기 있다.

| 반환 | 무엇인가 | 응답 | 지표 |
|---|---|---|---|
| `-4` | **다른 시도**가 이미 그 회원으로 받았다 | 4xx "이미 발급받으셨습니다" | `dupPerMember` |
| `-6` | **같은 멱등키**의 재시도, 이미 완료 | **최초 응답을 그대로 재사용**(DB 조회 1회) | `replayDone` |
| `-7` | **같은 멱등키**의 재시도, 아직 처리 중 | 409 + `Retry-After`. 폴링하지 않는다(§4.8) | `replayPending` |

멱등이 존재하는 이유가 **재시도를 안전하게 만드는 것**이므로, 응답을 못 받고 다시 누른 클라이언트에게 "이미 받으셨습니다"를 주면 그건 멱등이 아니라 고장이다. **세 카운터를 분리해서 낸다** — 합쳐 놓으면 `-4` 급증이 재시도에 묻혀 이상 신호를 놓친다.

`-6` 경로가 **DB 를 한 번 읽는 것은 정상**이다. 최초 응답 JSON 은 `idempotency_records` 에 있고 Redis 에 없다. "멱등을 Redis 로 옮겨 DB 조회가 사라졌다"는 **최초 요청에만** 해당한다(§4.6).

`SELECT … FOR UPDATE` 는 완전히 제거된다. `active_count` 갱신은 프로젝터로 빠진다(§9.6). 발급 트랜잭션에 남는 것은 세 테이블 INSERT 뿐이라 **v2 와 v3 의 영속화 단위가 같아진다**(§4.7).

**v1 과 배타 실행이어야 한다.** 같은 회차를 동시 처리하면 v1 의 행 잠금이 Redis 카운터를 모른 채 재고를 깎는다. 버전 전환은 회차 단위 토글로 하고, 전환 시점에 §6.2 재구성을 한 번 태운다.

### 4.4 완료·보상 스크립트 — 자기 토큰일 때만 건드린다

영속화가 끝나면 값을 `D` 로 올리고, 실패하면 되돌린다. **두 연산 모두 현재 값이 자기 선점일 때만 수행한다.** 무조건 `HSET` 하거나 접두만 맞으면 `HDEL` 하면, 되돌리기와 완료가 겹칠 때 **재고가 한 장 되살아난다.**

```lua
-- 완료 승격
-- KEYS[1]=issued  ARGV[1]=memberId ARGV[2]=requestToken ARGV[3]=nowMillis
local stored = redis.call('HGET', KEYS[1], ARGV[1])
if stored == false then return -1 end                      -- 사라졌다. 보상과 겹쳤다는 뜻
local st, ms, tk, key = string.match(stored, '^(%a)|(%d+)|([^|]+)|(.*)$')
if st == nil then return -3 end                            -- 값 형식 파손
if tk ~= ARGV[2] then return -2 end                        -- 남의 선점. 건드리지 않는다
if st == 'D' then return 0 end                             -- 이미 DONE. 재시도끼리 겹친 것
redis.call('HSET', KEYS[1], ARGV[1], 'D|' .. ARGV[3] .. '|' .. tk .. '|' .. key)
return 1
```

```lua
-- 보상
-- KEYS[1]=stock KEYS[2]=issued KEYS[3]=issued_ever
-- ARGV[1]=memberId ARGV[2]=requestToken
local stored = redis.call('HGET', KEYS[2], ARGV[1])
if stored == false then return 0 end                       -- 이미 없다
local st, ms, tk = string.match(stored, '^(%a)|(%d+)|([^|]+)|')
if st == nil then return -3 end                            -- 값 형식 파손
if tk ~= ARGV[2] then return 0 end                         -- 내 선점이 아니면 아무것도 안 한다
if st ~= 'P' then return -1 end                            -- 이미 DONE 이면 보상 금지. 경보
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('INCR', KEYS[1])
redis.call('DECR', KEYS[3])
return 1
```

보상은 **세 키를 전부 되돌린다** — "발급이 없었던 일"이므로. 취소는 `stock` 만 되돌린다 — "발급은 있었으나 재고는 반납"이므로. **이 비대칭을 틀리면 정합성 축 두 개가 조용히 어긋난다.**

**토큰 비교가 방어선이다.** 없으면 늦게 도착한 보상이나 중복 실행이 재고를 두 번 늘려 **초과 발급**이 난다. 멱등키만으로는 부족하다 — 같은 키의 서로 다른 시도를 구별하지 못하기 때문이다.

**음수 반환은 전부 이상 신호다.** 완료 `-1`·`-2`, 보상 `-1`, 양쪽 `-3` 은 정상 운영에서 0이어야 한다. 발생하면 카운터를 올리고 경보한다 — **삼키면 이 절의 CAS 가 아무 의미가 없다.**

**회수 표식을 `issued` Hash 에 남기지 않는다.** 상태는 `P`·`D` 둘뿐이다. `RECLAIMED` 같은 값을 남기면 `HLEN` 에는 계속 잡히는데 보상은 `issued_ever` 를 내리므로 `LUA_GAP` 이 즉시 어긋난다 — **감지 장치가 스스로 오작동을 만든다.** 흔적이 필요하면 TTL 붙은 별도 키에 남긴다.

보상 자체가 실패하면(Redis 무응답, 프로세스 사망) 재고가 영구 손실된다 — **과소 방향이라 불변식은 안 깨진다.** 회수는 §9.7.

### 4.5 실패 매트릭스

| 시점 | 상황 | 결과 | 회수 |
|---|---|---|---|
| Lua 전 | Redis 무응답 | redisCB OPEN → 503 | 신규 유입 차단 |
| Lua 중 | — | 원자적. 부분 적용 없음 | — |
| Lua 성공, INSERT 실패 | 제약 위반·타임아웃 | 보상 Lua → 5xx | 즉시 |
| INSERT 성공, 응답 전 사망 | 프로세스 킬 | 발급은 유효 | 재시도 → `-6` replay |
| INSERT 전 사망 | 프로세스 킬 | 재고 영구 손실(과소) | 재시도 시 자가 치유(§4.6) |
| 보상 실패 | Redis 무응답 | 재고 영구 손실(과소) | 재동기화 |
| 완료·보상 CAS 불일치 | 두 연산이 겹침 | 값을 건드리지 않고 이상 카운터 | **경보.** 정상 운영에서 0 |
| DB unique 위반 | Redis 중복 판정 통과 | 보상 후 409 | **Redis 상태 이상 — 경보.** 정상 운영에서 0 |

### 4.6 멱등 (D6)

Redis 가 대체하는 것은 `tryStart` 의 **게이트 역할뿐**이다. 레코드는 DB 에 유지한다.

| | 현행 담당 | v2 담당 |
|---|---|---|
| 동시 중복 차단 | `tryStart` (REQUIRES_NEW) | **Redis `HSETNX`** |
| replay 응답 저장 | `complete` (응답 JSON) | 그대로 DB |
| 본문 해시 비교 | `requestHash` | 그대로 DB |
| stale 회수 | `tryReclaim` | **자동 회수 없음**(§4.10). 계측만 하고 회수는 §9.7 |

`IdempotencyRepositoryImpl` 은 `JdbcTemplate` 만 쓰고 `@Transactional` 이 없어 호출한 트랜잭션에 합류한다. 분리를 만드는 것은 `IdempotencyClaimService` 의 `REQUIRES_NEW` 하나뿐이고, 그걸 안 타면 된다.

얻는 것 — 커밋 2회 → **1회**, `idempotency_records`·`IdempotencyResultCodec`·응답 JSON 그대로 유지, replay 응답이 최초 응답과 완전히 동일.

**자가 치유** — `-6 PENDING` 인데 DB 에 발급이 없으면 "선점은 내 것이고 발급은 없다"가 확정된다. 그 자리에서 INSERT 를 다시 시도하면 영구 손실이 재시도 한 번으로 회수된다.

**Redis 가 대신하지 못하는 것** — `-6` 의 응답 본문이다. 저장된 JSON 은 DB 에 있으므로 replay 는 `Redis 1회 + DB 1회` 다. 이 왕복을 줄이려고 응답 JSON 을 Redis 에 올리면 **Redis 가 또 하나의 원본이 되어 정합성 축이 늘어난다** — 하지 않는다(§7.5).

**대가** — 사용·취소·사용취소는 여전히 DB 멱등을 쓴다. 발급만 다르지만 이유가 명확하다(선착순 게이트가 있고 초당 수천 건이 몰린다). 그리고 `canonicalRequest` 해시 비교가 발급 경로에서 약해지는데, 본문이 사실상 `(couponRoundId, memberId, grade)` 이고 앞의 둘은 키·field 에 이미 있으며 `grade` 는 Lua 가 검증한다.

### 4.7 v2 와 v3 는 같은 영속화 코드를 쓴다

버전 사다리에서 갈리는 것은 **호출 지점과 배치 크기**여야지 영속화 구현이 아니다.

```
IssuancePersister.persist(List<PersistCommand>)     ← 한 트랜잭션
    ├ issuances           saveAll
    ├ issuance_histories  saveAll
    └ idempotency_records saveAll (DONE)

v2  persist(List.of(cmd))   크기 1 · 동기 · 요청 스레드
v3  persist(batch)          크기 N · 비동기 · Kafka consumer
```

v2 가 크기 1로 부르는 것은 마이크로배칭이 아니다 — 리스트를 받는 시그니처일 뿐 항상 1건을 동기로 쓴다.

v1 이 갈리는 것은 불가피하다. v1 의 트랜잭션에는 재고 차감이 반드시 들어가야 하고, 그것이 v1 의 정체성이다.

**추상화 클래스를 새로 만들지 않는다.** 공유는 이미 포트가 준다 — `IssuanceRepository`·`IssuanceHistoryRepository`·`IdempotencyRepository` 를 v2 도 v3 도 그대로 호출한다. 래퍼는 트랜잭션 경계에 이름을 붙일 뿐인데 **그 경계의 실패 의미가 둘에서 다르다** — v3 는 배치 N건 중 하나가 실패했을 때 전체 롤백인지 건별 격리 후 재시도인지를 정해야 하고, v2 의 크기 1에는 그 질문이 없다. 시그니처를 미리 맞춰 두면 v3 에서 갈아엎을 확률이 높다.

### 4.8 `-7 REPLAY_PENDING` — 폴링하지 않는다

`PENDING` 은 "누군가 선점했고 아직 영속 전"이다. v1 은 이 자리에서 폴링하며 기다렸다(`IdempotencyExecutionService` 의 wait-timeout·poll-interval). **v2 는 폴링하지 않는다.**

스파이크에서 폴링은 톰캣 워커 60개를 그대로 갉아먹는다. D6 이 "게이트만 대체"한다고 해서 이 비용까지 물려받을 이유가 없다. `PENDING` 이면 즉시 `409 Conflict` + `Retry-After: 1` 로 떨어뜨리고, 클라이언트가 다시 오면 그때는 대개 `DONE` 이라 `-6` 으로 갈린다.

### 4.9 보상 전 확인 — 불확실하면 보상하지 않는다

**INSERT 가 예외를 냈다고 커밋이 안 된 것은 아니다.** 타임아웃이나 커넥션 절단이면 커밋 여부를 모른다. 그 상태에서 보상을 쏘면 DB 에는 발급이 있고 Redis 에는 없다 — `ACTIVE_DB_GAP` 양수, 곧 **초과 발급 직전 상태**다.

```
INSERT 예외
 ├ ① issuances 를 (couponRoundId, memberId, idempotencyKey) 로 재조회
 │     있음 → 보상하지 않는다. 완료 CAS(§4.4)로 DONE 승격하고 그 결과로 응답
 │     없음 → 보상 Lua 실행
 └ ② 재조회 자체가 실패(DB 무응답) → 보상하지 않는다. PENDING 으로 남긴다. §4.10 이 세고, 회수는 §9.7
```

원칙 — **보상 누락은 과소(안전), 잘못된 보상은 초과 방향(위험).** 확실하지 않으면 하지 않는다.

### 4.10 PENDING 계측 — 자동 회수는 하지 않는다

선점하고 `DECR` 한 뒤 요청 스레드가 사라지면(GC pause, 워커 타임아웃, 프로세스 kill, 클라이언트 절단) 보상이 실행되지 않는다. field 는 `P` 로 남고 `issued_ever` 는 이미 올라갔으므로 **`PERSIST_GAP` 이 양수 = 재고 손실**이다.

`replay 자가 치유`(§4.6)는 **클라이언트가 같은 멱등키로 다시 와야만** 성립한다. 안 오면 안 낫는다. v1 에는 이 자리에 `isStale` + `tryReclaim`(`IdempotencyExecutionService:153,207`)이 있었고, v2 가 게이트를 Redis 로 옮기면서 그 대체물로 **주기적 스위퍼가 자동 회수하는 초안**이 있었다. **채택하지 않는다.**

**회수자와 원 요청이 경쟁하기 때문이다.**

```
요청 A                          회수자
Redis 선점 (DECR)
DB INSERT 시작
                                issuances 조회 → 행 없음
                                HDEL + INCR stock + DECR issued_ever
DB COMMIT 성공
완료 CAS → 값이 없어 실패 (-1)
```

회수자가 "DB 에 없다"를 확인한 시점과 A 가 커밋하는 시점 사이에는 **Redis 로 관측할 수 없는 창**이 있다. 그 창에서 회수가 성사되면 DB 에는 발급이 있고 재고는 한 장 되살아난다 — 다른 회원이 그 재고를 가져가는 순간 **`dbActiveCount > totalQuantity`, I1 초과 발급이 성립한다.**

`staleAfter` 를 늘려도 해결되지 않는다. GC pause·DB stall·커밋 응답 유실은 어떤 값이든 넘길 수 있어 **확률만 낮출 뿐**이다. §4.4 의 완료 CAS 가 "지워진 field 의 부활"은 막지만, **회수가 이미 `stock` 을 늘린 뒤라면 늦었다.**

**그래서 이 자리에서는 세기만 한다.**

```
주기(예: 30초)마다 OPEN 회차마다
  HSCAN cy:v2:issued:{r} COUNT 200        ← 커서 방식. 한 번에 200필드씩만 만진다
    상태가 P 이고 나이 > staleAfter 인 field 수를 센다
      → stalePendingCount 게이지로 노출. **값을 건드리지 않는다**
```

`HSCAN` 은 커서 방식이라 §3.3 의 O(N) 금지에 걸리지 않는다. 다만 hot key 를 만지므로 `COUNT` 로 쪼개고 **부하 중에는 주기를 늘린다.** `staleAfter` 는 v1 정책값을 그대로 쓴다 — 이제 회수 판정이 아니라 **집계 기준**이므로 값이 짧아도 위험하지 않다.

**실제 회수는 두 곳뿐이고, 둘 다 원 요청과 경쟁하지 않는다.**

| 회수 지점 | 왜 안전한가 |
|---|---|
| `-7` 재시도의 자가 치유(§4.6) | 회수자가 **원 요청 자신**이다. 경쟁할 상대가 없다 |
| §6.2 재구성 · §9.7 재동기화 | `meta` 를 지워 **발급이 멈춘 상태**에서 돈다. 커밋 중인 요청이 없다 |

**대가** — 죽은 선점의 재고가 캠페인 종료까지 묶인다. 과소 방향이라 불변식은 깨지지 않고, `stalePendingCount` 와 `PERSIST_GAP` 이 그 양을 그대로 보여준다. **되돌릴 수 없는 초과 발급 위험을, 되돌릴 수 있는 과소 재고와 맞바꾼 것**이다.

---

## 5. 재고 복원 (D3)

### 5.1 발급과 순서가 반대인 이유

| | 순서 | 근거 |
|---|---|---|
| 발급 | **Redis 먼저** → DB | 재고를 줄이는 방향. 게이트가 앞에 없으면 초과 발급 |
| 복원 | **DB 먼저** → Redis | 늘리는 방향. 늦어도 과소 표시일 뿐 안전 |

복원을 Redis 먼저 하면 DB 취소가 롤백됐을 때 재고만 늘어난 상태가 남는다. **불변식을 깨는 방향의 연산을 항상 나중에 한다** — 이 원칙 하나로 두 순서가 정해진다.

### 5.2 복원 스크립트

```lua
-- KEYS[1]=stock KEYS[2]=meta
-- 재발급 불가 정책이므로 issued Hash 와 issued_ever 는 건드리지 않는다
if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end       -- 게이트 미준비
local total = tonumber(redis.call('HGET', KEYS[2], 'totalQuantity'))
local left  = tonumber(redis.call('GET', KEYS[1]))
if left == nil or left >= total then return -2 end             -- 상한 초과 = 이상 신호
redis.call('INCR', KEYS[1])
return 1
```

`issued` Hash 와 `issued_ever` 를 두는 이유가 둘이다 — 1인 1매가 평생 기준이라 재발급이 막혀야 하고, 두 키가 `dbIssuedEverCount`(누적)의 짝이라 지우면 `LUA_GAP`·`PERSIST_GAP` 이 동시에 깨진다. `stock` 만 `dbActiveCount`(활성)의 짝이다.

`-2` 는 버그 신호다. DB 상태 전이(`CouponStateMachine`)가 이미 막지만, 카운터가 총재고를 넘는 순간 초과 발급이 확정되므로 Redis 에도 건다.

**만료 배치는 여러 장을 한 번에 되돌린다. 상한 검사가 같은 스크립트 안에 있어야 한다.**

```lua
-- 배치 복원  KEYS[1]=stock KEYS[2]=meta  ARGV[1]=건수
if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end       -- 게이트 미준비
local total = tonumber(redis.call('HGET', KEYS[2], 'totalQuantity'))
local left  = tonumber(redis.call('GET', KEYS[1]))
local n     = tonumber(ARGV[1])
if left == nil or n == nil or n <= 0 then return -3 end
if left + n > total then return -2 end                         -- 상한 초과 = 이상 신호
redis.call('INCRBY', KEYS[1], n)
return 1
```

`INCRBY` 를 그냥 쏘면 상한을 **한 번에 뛰어넘을 수 있다.** §5.1 이 복원을 "늘리는 방향이라 안전"으로 분류한 것은 **한 장씩 상한을 지킬 때**의 이야기이고, 재고 카운터가 총재고를 넘는 순간 초과 발급은 확정된다. `-2` 가 나오면 그 회차의 만료 처리를 멈추고 경보한다 — 그 시점에 이미 다른 곳이 깨져 있다는 뜻이라 §9.7 재동기화 대상이다.

### 5.3 손대야 하는 서비스

- `CouponCancelService` — 발급 취소
- `CouponCancelUseService` — 사용 취소(재고 복원 대상인지 상태 전이 규칙 재확인 필요)
- `CouponExpirationService` — 만료 배치. **건별이 아니라 회차별 건수를 모아 배치 복원 스크립트 한 번**(§5.2). 상한 검사가 그 안에 있다

세 곳 모두 **DB 커밋 이후**에 호출한다(`TransactionSynchronization.afterCommit`). 트랜잭션 안에서 부르면 롤백돼도 Redis 는 안 되돌아간다. Redis 실패는 전파하지 않는다 — 취소는 이미 커밋됐고 복원 누락은 과소라 안전하다. 실패 건수를 세고 §9.7 이 회수한다.

---

## 6. Redis 장애·재기동 (D4)

### 6.1 결정

**DB 집계로 재구성한다. replica 도 Sentinel 도 두지 않는다.**

D2(동기 INSERT)를 택한 시점에 성공한 발급은 예외 없이 `issuances` 에 남는다. Redis 의 어떤 키도 원본이 아니고 전부 DB 에서 다시 만들 수 있다.

### 6.2 재구성 절차

**키를 쓰는 순서가 전부다.**

```
1. cy:v2:meta:{r}       UNLINK              ← 게이트를 먼저 닫는다
2. issuances 에서 활성 수·누적 수·회원 목록을 한 트랜잭션으로 읽는다
3. cy:v2:issued:{r}     UNLINK 후 재작성 (HSET 1,000건 배치, 멱등키 자리는 '__rebuilt__')
   cy:v2:issued_ever:{r} = 누적 건수         ← 3번과 반드시 함께
4. cy:v2:stock:{r}      = total_quantity − 활성 건수
   coupon_stocks.active_count = 활성 건수    ← DB_COUNTER_GAP 까지 정리
5. cy:v2:meta:{r}       작성                ← 게이트를 마지막에 연다
```

1·5 가 안전장치다. `meta` 가 없으면 Lua 가 `-9` 를 반환해 그 회차 발급이 전부 503 으로 떨어진다. **재구성 중에는 아무도 낡은 카운터를 볼 수 없고**, 도중에 죽어도 게이트가 닫힌 채 남아 안전하다.

3번에서 `issued_ever` 를 빠뜨리면 그 순간 `LUA_GAP ≠ 0` — **재구성 자체가 정합성 사고**가 된다.

**게이트는 발급만 막는다.** 취소·사용취소·만료는 계속 돌고 DB 커밋도 계속 된다. 복원 스크립트(§5.2)가 `meta` 부재를 보고 `-1` 을 반환해 Redis `INCR` 은 건너뛰지만, **2번 집계 이후 커밋된 복원분은 4번 계산값에 안 들어간다** — 그 회차의 재고가 그만큼 적게 복구된다.

```
4′. cy:v2:meta 를 쓰기 직전에 활성 집계를 다시 읽어 stock 을 갱신한다
    게이트가 닫혀 발급이 없으므로 활성 수는 단조 감소만 하고 곧 수렴한다
    복원 경로가 -1 을 반환한 건수를 카운터로 남겨 수렴 여부를 확인한다
```

4′ 를 넣지 않으면 재구성 창(2번~5번) 동안의 취소가 조용히 유실된다.

활성 집계가 `CANCELED` 를 제외하는 이유는 §5.1 과 같다. 단 `issued` Hash 에는 **포함시킨다**(재발급 불가). 그래서 2번의 두 쿼리 조건이 다르다.

같은 절차를 캠페인 오픈 T-30초 워밍업에도 쓴다. 코드 경로가 하나다.

**api 가 여러 대이므로 동시 실행을 반드시 막는다.** 두 인스턴스가 겹쳐 돌면 한쪽이 5번으로 게이트를 연 뒤 다른 쪽이 4번을 덮어써, **발급이 도는 중에 카운터가 갈아엎힌다** — 초과 발급 방향이다. 두 가지 중 하나를 택한다.

- **batch 단독 소유** — batch 는 1대다. 재구성·재동기화를 batch 만 수행하면 락 자체가 필요 없다. 권장.
- **Redis 락** — `SET cy:v2:rebuild:{r} <ownerToken> NX PX <최악의 재구성 시간보다 길게>`. **단계마다 소유를 다시 확인**하고, 해제는 자기 토큰일 때만 한다(§4.4 와 같은 원리).

### 6.3 느려지는 Redis

프로세스가 죽는 것보다 흔한 건 죽지 않고 느려지는 것이다(eviction, AOF rewrite fork, 누가 `KEYS` 를 때림). Sentinel 은 이걸 장애로 판정하지 않는다.

- Lua 호출 타임아웃 **100ms**. 초과하면 실패로 간주
- 연속 실패가 임계를 넘으면 **redisCB OPEN → 503**. 매달려 톰캣 스레드를 소진시키는 것보다 낫다
- `slowlog-log-slower-than` **1000µs**
- `maxmemory-policy` **`noeviction`**. 조회 캐시가 재고 키를 evict 하면 그 자체가 사고다. 캐시 키는 전부 TTL 로 스스로 사라지게 한다

### 6.4 가용성 장치를 얹지 않은 이유

| | 가용성 | 정합성 | 노드 |
|---|---|---|---|
| AOF everysec 만 | ✗ | 1초치 유실 → **초과 발급 방향** | +0 |
| + read replica | ✗ | 유실 그대로 | +1 |
| + Sentinel | ✓ | 유실 그대로 | +3 |
| Cluster | ✓ | 유실 그대로 | 무의미(§3.3) |
| **DB 재구성** | ✗ | **완전** | +0 |

**replica·Sentinel·Cluster 는 전부 가용성 장치이고 정합성은 하나도 해결하지 않는다.** Redis 복제는 비동기라, 마스터가 `DECR` 응답을 보낸 뒤 그 명령이 replica 에 닿기 전에 죽으면 승격된 replica 에서 그 발급들이 없던 일이 되어 재고가 되살아난다. `WAIT 1 100` 은 왕복이 붙고 합의도 아니다.

재구성과 replica 는 배타적이지 않다. 나중에 얹더라도 §6.2 는 **failover 유실의 최종 회수 수단**으로 남는다.

---

## 7. 조회 설계 (D5)

### 7.1 지금 쿼리를 그대로 캐싱할 수 없는 이유

```sql
       stock.total_quantity - stock.active_count AS remainingQuantity  -- ① 매 순간 변함
  FROM coupons JOIN coupon_stocks ...
 WHERE coupon.open_at <= :asOf AND coupon.close_at > :asOf             -- ② 시각 파라미터
   AND NOT EXISTS (SELECT 1 FROM issuances
                    WHERE coupon_id = coupon.id AND member_id = :memberId)  -- ③ 회원별
```

**세 축을 분리한다.**

```
[A] 쿠폰 정의 — 회차+템플릿. 재고·발급여부 제외.  키: cy:v2:def:{gradeBit}:{page}
[B] 잔여 재고 — cy:v2:stock:{r} 직독                (v2 자료구조 재사용)
[C] 발급 여부 — cy:v2:issued:{r} 의 HEXISTS         (v2 자료구조 재사용)
```

**v2 가 만든 자료구조가 그대로 조회 최적화 재료가 된다.** ①③이 Redis 로 빠지면 남는 쿠폰 정의는 등급 4종 × 페이지라 키가 20개 남짓이다. `asOf` 는 초 단위로 절삭해 키에서 뺀다.

```
L1 히트   : Redis 1회 (재고·발급여부 파이프라인)   DB 0회
L2 히트   : Redis 2회 (＋ 쿠폰 정의 GET)           DB 0회
전부 미스 : 위 + DB 1회 (쿠폰 정의만)
```

### 7.2 2계층 — L1 힙 · L2 Redis

```
GET /coupon-rounds
  ├ L1  Caffeine.get(key, loader)     api 힙 · TTL 10s        왕복 0
  └ 미스 → loader:
        ├ L2  Redis GET cy:v2:def:…   TTL min(다음 경계, 30s)  ~0.3ms
        └ 미스 → MySQL SELECT → Redis SETEX                    수 ms
```

각 층이 아래층의 부하를 흡수하고 **DB 는 두 층을 다 뚫고 온 요청만 본다.**

| 조회 | L1 | L2 | 이유 |
|---|---|---|---|
| 공개 회차 목록 | ✅ | ✅ | 회원 축 없음 |
| 발급 가능 회차 | ✅ | ✅ | 등급 4종 × 페이지 |
| 회차 상세 | ✅ | ✅ | 단건 |
| **내 쿠폰함** | ❌ | ✅ | **회원 100만. L1 에 담으면 힙이 터진다** |

**single flight 는 층마다 따로 걸린다.**

| 층 | 막는 것 | 수단 | 지금 |
|---|---|---|---|
| L1 미스 | 같은 인스턴스의 스레드들이 동시에 아래층을 침 | **Caffeine `get(key, loader)` 내장** | 필요 |
| L2 미스 | 여러 api 인스턴스가 동시에 DB 를 침 | Redis 분산 락 | **api 1대라 불필요** |

```java
Cache<String, Payload> l1 = Caffeine.newBuilder()
        .expireAfterWrite(Duration.ofSeconds(10)).maximumSize(1_000).build();

Payload get(String key) {
    return l1.get(key, k -> {                    // ← 여기서 single flight 가 보장된다
        Payload cached = redis.get(k);
        if (cached != null) return cached;
        Payload fresh = readFromDatabase(k);
        redis.setex(k, ttlWithJitter(k), fresh);
        return fresh;
    });
}
```

api 를 N대로 늘리면 추가되는 것은 둘뿐이다 — L2 미스 경로에 Redis 분산 락, L1 evict 를 Pub/Sub 브로드캐스트. 계층 구조는 그대로다.

### 7.3 SWR 미채택 — stale-if-error 만

쿠폰 정의는 캠페인이 도는 동안 안 변하고, 변하는 계기는 `open_at`·`close_at` 도달·회차 생성 셋뿐이며 **전부 시각이 예정된 사건**이다. 언제 낡는지 아는데 낡은 채로 쓸 이유가 없다 — `TTL = min(다음 경계까지, 30s)` 가 SWR 보다 정확하다.

| | 언제 낡은 값을 주나 | 목적 |
|---|---|---|
| SWR | **평소에도** | 아무도 안 기다리게 |
| **stale-if-error** | **갱신이 실패·지연할 때만** | 장애 때 안 죽게 |

남기는 이유는 하나 — 스파이크 중 MySQL 이 발급 쓰기로 포화라 쿠폰 정의 SELECT 가 수백 ms 걸릴 때 대기자들이 예산을 넘긴다. 부수 효과로 측정 해석이 깨끗해진다(낡은 값은 고장났을 때만 나가므로 p99 를 의심할 필요가 없다).

**내 쿠폰함은 stale-if-error 대상에서도 제외한다.** 장애 상황이라도 자기 쿠폰이 안 보이는 건 다른 화면보다 훨씬 나쁘다.

TTL 에 **±10% 지터**.

### 7.4 무효화

```
회차 lifecycle 변경   →  Redis DEL + L1 evict
발급 · 사용 · 취소     →  해당 회원 쿠폰함 키 Redis DEL (L1 대상 아님)
만료 배치             →  무효화하지 않는다
api N대가 되면        →  L1 evict 를 Pub/Sub 브로드캐스트
```

만료 배치가 빠지는 이유 — 만료 표시는 조회 시점에 `expires_at < now` 로 계산한다(PRD 만료 3계층 중 3계층). **캐시에 담는 건 원본 행이지 계산된 만료 여부가 아니다.**

### 7.5 캐시는 표시용, 판정은 게이트

**어떤 값이든 발급 성립 여부를 좌우하면 캐시에 넣지 않는다.**

Redis 가 흔히 "캐시 서버"로 불리지만 **우리 Redis 키의 대부분은 캐시가 아니다.**

| 키 | 용도 | TTL | 없어지면 |
|---|---|---|---|
| `cy:v2:stock:{r}` | 재고 카운터 | **없음** | 발급 불가 |
| `cy:v2:issued_ever:{r}` | 누적 카운터 | **없음** | 정합성 축 계산 불가 |
| `cy:v2:issued:{r}` | 발급자 · 멱등 게이트 | **없음** | 발급 불가 |
| `cy:v2:meta:{r}` | 게이트 상태 | **없음** | 발급 불가(503) |
| `cy:v2:def:{grade}:{page}` | **쿠폰 정의 — 진짜 캐시** | 30s | DB 에서 읽으면 됨 |

**맨 아래 하나만 캐시이고, 그것만 L1/L2 에 올라간다.**

실제로 위험해지는 경로가 하나 있다 — §7.1 원본 SQL 의 `stock.active_count < stock.total_quantity` 를 **쿠폰 정의 쿼리에 남겨두는 것**. 그러면 재고가 캐시에 스며든다. 분해할 때 이 조건은 반드시 Redis 쪽으로 옮긴다.

**stale 이 만들 수 있는 최악은 헛클릭이다.** 닫힌 회차는 `-1`, 매진은 `-5`, 이미 받았으면 `-4` 로 게이트가 거른다. 재고를 한 장도 더 내주지 않는다. **거절 코드 분포를 지표로 둔다** — `-5`·`-1` 비율이 튀면 TTL 이 길다는 신호다.

### 7.6 Redis 가 죽으면

```
발급   redisCB OPEN → 503                (게이트가 없으므로 불가피)
조회   L1 히트 → 그대로 응답              ← 살아 있다
       L1 미스 → L2 실패 → DB 직행 (single flight 가 1건으로 묶음)
```

**L1 이 Redis 장애의 완충이다.** 단층이었다면 캐시·stale·조회가 한꺼번에 무너져 발급이 멈춘 와중에 조회까지 DB 로 폭주한다.

반대 대가 — L2 는 발급 게이트와 같은 인스턴스라 단일 스레드를 공유한다. §3.3 의 O(N) 금지가 캐시 값에도 적용된다(페이지당 20회차 ≈ 4KB 라 현재는 문제없음).

**혼합 시나리오의 Redis 예산을 명시한다.**

```
실측 여유                    112,941 rps
발급  20,000 rps × 1왕복  =   20,000
────────────────────────────────────────
조회에 남는 몫               92,941 rps

조회 1건이 쓰는 왕복
  L1 히트   1회 (재고 MGET + 발급여부 파이프라인)
  L2 히트   2회
  전부 미스 2회 + DB 1회
```

**이 계산은 왕복 수 기준이다.** Redis 는 명령 단위로 직렬화되므로 서버가 하는 일의 양은 다르다 — 목록에 회차 20개면 `MGET` 1 + `HEXISTS` 20 = **약 21 명령**이고 왕복은 1회다. 회원축 인덱스로 이를 줄이는 안은 §0.1 에서 반려했으므로, **측정에서는 왕복이 아니라 명령 수로 예산을 확인한다.**

L1 적중률을 h 라 하면 조회 QPS Q 의 소비량은 대략 `Q × (2 − h)`. h=0.9 이면 `Q × 1.1` 이므로 **조회 QPS 상한이 약 84,000** 이다. 혼합 시나리오에서 실제 Q 를 실측해 이 예산 안에 있는지 확인한다 — **혼합이 정식 시나리오인 이상 이 계산이 없으면 §3.1 의 "여유가 있다"가 발급 단독 기준의 주장으로 남는다.**

### 7.7 재고 조회

`cy:v2:stock:{r}` 직독. 목록에 회차 N개면 `MGET` 한 번. 캐시가 아니다.

---

## 8. v1 정비 — v2 착수 전에

V1-2 가 이미 상당 부분을 해결했다. 남은 것은 셋이다.

| # | 위치 | 문제 | 조치 |
|---|---|---|---|
| **A** | `CouponIssueService:83~90` | 매진 판정(`occupyOne`)이 맨 마지막이라 **매진 요청도 `issuances` + `histories` INSERT 두 건을 실행하고 롤백**한다. V1-2 가 `histories` 를 `occupy` 앞으로 옮기면서 한 건 더 늘었다 | `occupyOne` 을 INSERT 앞으로. 매진이면 INSERT 0건 |
| **G** | `CouponIssueService` vs `CouponCancelService:93` · `CouponCancelUseService:131` · `CouponExpirationService:95` | **잠금 순서 역전** — 아래 | 순서를 다시 통일하거나, 데드락이 실제로 나는지 실측 후 판단 |
| D | `PolicyValidator` + `CouponIssueService:58` | 회차 조회·정책 검증이 각각 두 번 | 읽기라 fsync 를 안 내므로 급하지 않다. v2 는 Lua `meta` 가 대체해 자연 소멸 |

**V1-2 에서 이미 해결된 것** — `NOT_FOUND` 죽은 분기(`existsById` 로 구분해 반환), 코드 생성이 락 보유 중이던 문제(락이 없어져 자연 해소).

### 8.1 잠금 순서 역전 (G)

V1-2 가 삭제한 주석이 이 불변을 명시하고 있었다.

> `재고 행을 먼저 잠가 발급·취소·만료 경로의 잠금 순서를 통일한다`

**발급만 순서를 뒤집고 나머지 셋은 그대로 뒀다.** `lockForUpdate` 는 포트에도 남아 있고 세 서비스가 여전히 쓴다.

```
발급      issuances → histories → stock          (stock 을 마지막에 잠근다)
취소      stock(FOR UPDATE) → issuances          ← lockForUpdate 유지
사용취소  stock(FOR UPDATE) → …                   ← 유지
만료      stock(FOR UPDATE) → …                   ← 유지
```

**이것은 V1-2 전용 위험이다.** v2 에서는 발급이 `coupon_stocks` 를 아예 안 건드리므로(§9.6 에서 `release` 도 걷어낸다) 데드락 후보 조합 자체가 없다. V1-2 를 기준선으로 측정하는 동안만 문제다.

**실제 데드락 여부는 실행 계획에 달렸다.** `updateStatusIfCurrent` 가 PK 로 접근하면 발급의 유니크 인덱스 갭 락과 안 부딪힐 수 있다. 다만 `LATEST DETECTED DEADLOCK` 확인은 **음성 결과가 증거가 되지 않는 검사**다 — 데드락은 확률적이라 "안 났다"로 "없다"를 만들 수 없다. **V1-2 회차에서 데드락이 한 번이라도 나면 그 회차 수치는 버린다**는 규칙을 미리 정해 두는 편이 낫다.

다만 **명시적으로 지키던 불변을 포기했는데 그 대가가 커밋 메시지에도 주석에도 남아 있지 않다.** 의도한 것이라면 근거를, 아니라면 순서를 되돌려야 한다.

### 8.2 v1 의 정체성이 바뀌었다

PRD 는 v1 을 이렇게 정의한다.

> `v1 · DB 비관적 락` — `SELECT ... FOR UPDATE` / 관찰 — 커넥션 풀 고갈, **락 대기 큐**

**V1-2 에는 `FOR UPDATE` 가 없다.** 조건부 원자 UPDATE 만 남았고 이건 비관적 락이 아니다. 락 보유 구간이 짧아져 v1 이 빨라지고 **v2 와의 격차가 줄어든다.**

V1-1 과 V1-2 를 둘 다 측정하면 사다리가 4단이 된다.

```
V1-1  비관적 락 (FOR UPDATE 로 먼저 잠그고 그 안에서 INSERT)
V1-2  원자 조건부 UPDATE (락 보유 구간 축소)
V2    Redis Lua
V3    Redis + Kafka
```

이 경우 PRD 의 "v1 = 비관적 락" 서술을 V1-1 로 좁히고, V1-2 를 "락 보유 구간을 줄인 중간 단계"로 문서에 추가해야 한다. **미결(§11).**

### 8.3 관측은 이미 비동기다

`AttemptEventPublisher` 가 Kafka 로 논블로킹 produce 만 하고, `AttemptLiveSink`(Redis Stream)·`AttemptArchive`(`issue_attempts` INSERT)는 **Kafka consumer** 가 쓴다. 발급 요청 스레드는 DB 를 안 친다 — "거절은 DB 왕복 0회"가 성립하는 근거.

미확인 — Kafka producer `max.block.ms`. 브로커가 죽고 버퍼가 차면 `send()` 가 블로킹돼 톰캣 워커를 잡는다.

---

## 9. 정합성

`ConsistencyGapType.isApplicable` 이 **V2 에 gap 4축을 전부 적용**한다고 못 박아 두었다. v2 의 의무는 그 계약에 원천값을 정확히 공급하는 것이다.

### 9.1 불변식

| | 불변식 | 판정식 | 위반의 의미 |
|---|---|---|---|
| **I1** | 초과 발급 0 | `overIssued = max(0, dbActiveCount − totalQuantity)` | 없는 재고를 팔았다 |
| **I2** | 1인 1매(평생) | `uk_coupon_member` 위반 0건 | 정책 붕괴 |
| **I3** | 미영속 발급 0 | `PERSIST_GAP = redisIssuedEver − dbIssuedEver = 0` | 선점됐는데 DB 에 없다 |
| **I4** | Redis 내부 일관 | `LUA_GAP = redisIssuedEver − redisMemberEver = 0` | Lua 원자성이 깨졌다 |
| **I5** | Redis↔DB 일치 | `ACTIVE_DB_GAP = (total − redisRemaining) − dbActive = 0` | 두 저장소가 갈라졌다 |
| **I6** | DB 내부 일관 | `DB_COUNTER_GAP = dbActive − storedActive = 0` | 집계 컬럼이 실제와 다르다 |

`FINAL` 은 **적용 가능한 gap 이 하나라도 0이 아니면 즉시 FAIL**. 임계도 허용 오차도 없다.

I1·I2 는 사고이고 I3~I6 은 사고의 전조다. **I3~I6 을 실시간 감시해 I1·I2 가 일어나기 전에 잡는 것**이 목표다.

### 9.2 원천값 공급 계약

| 원천값 | 공급처 | 집계 범위 |
|---|---|---|
| `totalQuantity` | `coupon_stocks.total_quantity` | — |
| `redisRemaining` | `GET cy:v2:stock:{r}` | 활성 |
| `redisIssuedEverCount` | `GET cy:v2:issued_ever:{r}` | 누적 |
| `redisMemberEverCount` | `HLEN cy:v2:issued:{r}` | 누적 |
| `dbActiveCount` | `COUNT(issuances) WHERE status IN (ISSUED, USED)` | 활성 |
| `dbIssuedEverCount` | `COUNT(issuances)` 전체 | 누적 |
| `storedActiveCount` | `coupon_stocks.active_count` | 활성 |

```
활성(active) = ISSUED + USED                        ← stock 카운터의 짝
누적(ever)   = ISSUED + USED + CANCELLED + EXPIRED  ← issued Hash · issued_ever 의 짝
```

| 연산 | stock | issued_ever | issued Hash |
|---|---|---|---|
| 발급 성공 | `DECR` | `INCR` | `HSETNX` |
| 발급 보상 | `INCR` | `DECR` | `HDEL` |
| 취소·사용취소·만료 | `INCR` | — | — |

### 9.3 4계층 방어

| 불변식 | L1 구조 | L2 감지 | L3 회수 | L4 증명 |
|---|---|---|---|---|
| I1 | Lua 원자성 · `CHECK` · 보상 멱등성 | `overIssued` → 즉시 CRITICAL | 재구성 | 규칙 V1 `STOCK_MISMATCH` |
| I2 | `HSETNX` ＋ DB `UNIQUE` | unique 위반 카운터 | DB 가 물리적 차단 | 규칙 V2 `DUP_PER_MEMBER` |
| I3 | 동기 INSERT ＋ 보상 | `PERSIST_GAP` | replay 자가 치유 | 규칙 V3 `ORPHAN_COUPON` |
| I4 | 세 쓰기를 한 스크립트에 | `LUA_GAP` → 즉시 CRITICAL | 재구성 | — |
| I5 | 연산 방향 규칙(§5.1) | `ACTIVE_DB_GAP` → 임계 10/100 | 재동기화 | 규칙 V1 |
| I6 | 프로젝터(§9.6) | `DB_COUNTER_GAP` | 프로젝터 | 규칙 V1 |

**L1 에 무게를 싣는다.** 감지는 이미 벌어진 일을 알려줄 뿐이라, 초과 발급처럼 되돌릴 수 없는 사고는 구조로 막아야 한다.

### 9.4 각 gap 이 잡는 실패 모드

| gap | 부호 | 무엇이 고장났는가 | LIVE 심각도 |
|---|---|---|---|
| `LUA_GAP` | ≠0 | **Lua 원자성** 또는 재구성이 두 키를 함께 안 세움 | **즉시 CRITICAL** |
| `PERSIST_GAP` | 양수 | 선점 후 INSERT 실패 + 보상 실패. **재고 영구 손실** | 임계 10/100 |
| | 음수 | Redis 에 없는 발급이 DB 에. 재구성 누락 또는 v1/v2 동시 실행 | 〃 |
| `ACTIVE_DB_GAP` | 양수 | **초과 발급 직전 상태** | 임계 10/100 |
| | 음수 | 복원 경로가 Redis 에 미반영 | 〃 |
| `DB_COUNTER_GAP` | ≠0 | DB 내부 불변식 위반 | V1 즉시 CRITICAL / **V2·V3 임계 기반**(§9.6) |

`LUA_GAP` 은 Redis 안에서만, `DB_COUNTER_GAP` 은 DB 안에서만 계산된다. **한 저장소 안의 두 값이 어긋났다면 지연이 아니라 버그다.**

### 9.5 관측 스큐 — 읽기 규칙

Redis 와 DB 를 동시에 정지시켜 읽을 수 없다. 통제하지 않으면 정상 상태를 사고로 오독한다.

**규칙 1 — 같은 저장소 안의 값은 원자적으로 읽는다.**

```lua
-- KEYS[1]=stock KEYS[2]=issued_ever KEYS[3]=issued
return { redis.call('GET', KEYS[1]),
         redis.call('GET', KEYS[2]),
         redis.call('HLEN', KEYS[3]) }
```

`dbActiveCount` 와 `storedActiveCount` 도 **하나의 읽기 트랜잭션**에서 뽑는다.

**규칙 2 — 저장소 사이는 스큐 방향을 고정한다.**

```
Redis 먼저 → DB 나중, 그 사이 k건 발급  ⇒ gap ≈ −2k  (음수 = 과소 = 안전)
DB 먼저   → Redis 나중                  ⇒ gap ≈ +2k  (양수 = 초과발급처럼 보임)
```

> **항상 Redis 를 먼저 읽고 DB 를 나중에 읽는다.**
> 스큐가 안전한 음수 방향으로만 치우치므로, **양수 gap 은 스큐로 설명되지 않는 진짜 이상 신호**가 된다.

**규칙 3 — 스큐 상한은 `2λΔ`.** 발급률 3,000/s · 간격 5ms 면 30 이라 WARN 임계 10 을 이미 넘는다. 그래서 LIVE 임계는 정지 상태 기준이 아니다.

### 9.6 LIVE 와 FINAL, 그리고 `DB_COUNTER_GAP` (D9)

| | LIVE | FINAL |
|---|---|---|
| 시점 | 부하 중, 1초 주기 | 부하 종료 + 안정화 후 |
| 스큐 | 불가피. 최대 `2λΔ` | 없음 |
| 판정 | 임계 기반 severity | **gap 전부 정확히 0** |

정확한 명제 — 부하 중에는 gap 이 `±2λΔ` 에서 흔들리되 **양수로는 치우치지 않고**(규칙 2), λ가 0이 되면 **정확히 0으로 수렴**한다.

**FINAL 진입 게이트** (호출자가 확인): ① 정적 구간 5초 이상(진행 중 보상까지 종료) ② 인플라이트 0 ③ `dbActiveCount` 를 집계 컬럼이 아니라 `issuances` 실 COUNT 로 ④ **`stalePendingCount` 가 0**(§4.10). 0이 아니면 §9.7 재동기화를 한 번 태운다 ⑤ **프로젝터가 마지막 발급 이후 1회 완주**(§9.6).

④⑤ 가 없으면 FINAL 이 구조적으로 실패한다. `hasFinalMismatch` 에는 **버전별 완화가 없기 때문**이다.

```java
// hasFinalMismatch — applicable 이면 무조건 0 이어야 한다
.filter(entry -> entry.getKey().isApplicable(engineVersion))
.anyMatch(entry -> entry.getValue().value() != 0);

// isApplicable — V2 는 네 축 전부
case V2, V3 -> true;
```

LIVE severity 를 아무리 완화해도 FINAL 은 영향을 안 받는다. **`DB_COUNTER_GAP` 이 asOf 시점에 수렴해 있지 않으면 FINAL 은 무조건 FAIL 이다.** 이 축을 끄지 않고 FINAL 을 구하는 유일한 방법이 ⑤ 다.

**D9 — `active_count` 갱신 주체.** v2 는 `FOR UPDATE` 를 없애므로 갱신자가 사라진다.

발급 트랜잭션에 `UPDATE coupon_stocks SET active_count = active_count + 1` 을 넣으면 회차 행에 X 락이 걸리고 커밋까지 유지된다.

**"락이 부활한다"는 부정확한 표현이다.** V1-2 의 `occupyOne` 이 이미 같은 행에 같은 UPDATE 를 하고 있다 — 부활하는 것은 비관적 락이 아니라 행 X 락이고, 그건 어떤 UPDATE 든 잡는다. 정확한 대가는 이것이다.

> v2 에 `active_count` UPDATE 를 넣으면 **회차 행 직렬화가 V1-2 와 정확히 같아진다.**
> v2 의 개선은 거절 경로·정책 검증·멱등 커밋에서만 나오고, "재고 점유의 직렬화를 없앴다"는 주장은 사라진다.

처리량 손실 폭은 **추정이다.** 커밋 fsync 0.3~0.6ms 를 락 보유 구간으로 보면 회차당 1,667~3,333 TPS 이지만 실측이 아니다. 다만 위 서사 손실만으로도 프로젝터를 택할 근거는 충분하다.

대신 이 축의 의미가 v2 에서 달라졌음을 반영한다.

| | `active_count` 의 역할 | 틀렸을 때 |
|---|---|---|
| v1 | 재고 판정의 **주체** | 곧바로 초과 발급 — 치명 |
| v2 | 재고 판정은 Redis 가 한다. **파생 집계** | 발급에 영향 없음 — 지연 |

**결정:**

① 프로젝터가 주기적으로 `issuances` 실 COUNT 로 덮어쓴다.

```sql
UPDATE coupon_stocks s
   SET s.active_count = (SELECT COUNT(*) FROM issuances i
                          WHERE i.coupon_id = s.coupon_id
                            AND i.status IN ('ISSUED','USED'))
 WHERE s.coupon_id = ?
```

Redis 를 경유하지 않는 것이 중요하다. `total − redisRemaining` 으로 덮으면 이 축이 `ACTIVE_DB_GAP` 과 중복된다. 락 경합은 회차당 초당 1회라 무시할 수준이다.

② `severityForGap` 의 **구조를 바꾼다.** 지금은 `DB_COUNTER_GAP` 이 버전 분기 **이전에** 조기 반환되어 V2·V3 임계 분기에 도달조차 못 한다.

```java
// 지금 — 버전 분기에 못 간다
if (gapType == LUA_GAP || gapType == DB_COUNTER_GAP) return Severity.CRITICAL;
if (engineVersion == V3) { ... }
if (engineVersion == V2) { ... }

// 바꾼 뒤 — 조기 반환에서 DB_COUNTER_GAP 을 뺀다
if (gapType == LUA_GAP) return Severity.CRITICAL;                    // Redis 원자성은 버전 무관
if (gapType == DB_COUNTER_GAP && engineVersion == V1) return CRITICAL; // v1 은 판정 주체였다
// 이하 V2·V3 임계 분기로 흐른다
```

③ **FINAL 은 그대로 0을 강제한다.** `hasFinalMismatch` 는 손대지 않고, 대신 FINAL 진입 게이트 ⑤(프로젝터 수렴)로 0을 만든다. `isApplicable` 에서 V2 의 `DB_COUNTER_GAP` 을 빼는 선택은 **감지 축 하나를 끄는 것**이라 택하지 않는다.

④ **`CouponStockRepository.release()` 의 DB UPDATE 도 v2 에서 걷어낸다.**

프로젝터만 쓰기로 하면 발급은 `active_count` 를 안 올리는데 취소는 `active_count = active_count - n WHERE active_count >= n` 으로 내린다. 프로젝터가 아직 발급분을 반영하지 않은 구간에 취소가 오면 **`WHERE` 가 거짓이 되어 0행 → `COUPON_STOCK_RELEASE_FAILED` → 정상 취소가 실패한다.**

절대값 쓰기(프로젝터)와 상대값 쓰기(`release`)를 섞으면 안 된다. v2 에서는 `release` 의 DB 갱신을 빼고 Redis `INCR`(§5.2)만 남기며, `active_count` 는 프로젝터가 단독으로 소유한다. `CHECK (0 ≤ active_count ≤ total_quantity)` 도 절대값 쓰기에서는 위반되지 않는다.

대가 — `WHERE active_count < total_quantity` 가 주던 DB 레벨 방어 한 겹이 사라진다. I1 의 실질 방어는 Lua 원자성과 `overIssued` 감시다.

### 9.7 재동기화

§4.4·§5.3 의 잔여 오차를 되돌린다. **§6.2 재구성과 같은 코드**를 쓴다 — 게이트를 닫고, DB 에서 다시 만들고(세 키 + `active_count`), 게이트를 연다. gap 네 축을 **동시에** 0으로 되돌리는 유일한 지점이다.

부하 중에는 돌리지 않는다. 캠페인 종료 후 또는 CRITICAL 초과 시 수동으로.

### 9.8 검증 배치·오염셋

| 오염 유형 | 규칙 | v2 에서 이걸 만드는 실패 |
|---|---|---|
| 1-b 쿠폰 행 없이 카운터만 틀림 | V1 `STOCK_MISMATCH` | 보상 실패로 인한 재고 영구 손실 |
| 1-a 쿠폰 행은 있는데 이력 없음 | V3 `ORPHAN_COUPON` | 선점 후 history INSERT 만 실패 |
| 6 동일 유저 2건 | V2 `DUP_PER_MEMBER` | Redis 중복 판정 통과 + DB unique 부재 시 |

**v2 가 만들 수 있는 오류가 이미 오염셋에 있다.** 새 유형은 필요 없고, v2 특유의 실패를 실제로 주입해 규칙이 잡는지 확인하는 게 남는다.

### 9.9 정합성 테스트

| # | 주입할 고장 | 방법 | 기대 감지 |
|---|---|---|---|
| C1 | 선점 후 INSERT 실패 | INSERT 직전 예외 + 보상 비활성 | `PERSIST_GAP` 양수, `ACTIVE_DB_GAP` 양수 |
| C2 | 보상 이중 실행 | 보상 두 번 호출 | 두 번째가 0 반환, gap 불변 |
| C3 | 재구성이 `issued_ever` 누락 | 3번에서 키 생략 | `LUA_GAP ≠ 0` → 즉시 CRITICAL |
| C4 | 복원이 Redis 미반영 | `afterCommit` 훅 비활성 | `ACTIVE_DB_GAP` 음수 |
| C5 | v1·v2 동시 실행 | 같은 회차를 두 경로로 | `PERSIST_GAP`·`ACTIVE_DB_GAP` 음수 |
| C6 | Redis 중복 판정 우회 | `issued` Hash 강제 삭제 후 재발급 | DB `UNIQUE` 위반 → 409 + 이상 카운터 |
| C7 | 스큐 방향 검증 | 부하 중 gap 부호 분포 | 양수 관측 0건(규칙 2) |
| C8 | FINAL 게이트 미충족 | quiet period 없이 FINAL | `FINAL_VALUE_UNAVAILABLE` |
| C9 | **회수와 커밋의 인터리브** | 회수 로직을 강제 호출한 직후 원 요청 커밋 | 초과 발급 0. 완료 CAS 가 `-1` 반환 + 이상 카운터 |
| C10 | **보상된 field 에 DONE 승격** | 보상 실행 후 완료 CAS 호출 | field 가 **부활하지 않는다.** `-1` 반환 |
| C11 | **배치 복원 상한 초과** | `stock + n > total` 이 되도록 만료 배치 주입 | `-2` 반환 + 경보. `stock` 불변 |
| C12 | **멱등키 접두 충돌** | 같은 회원, `stored=abcdef…` 에 `request=abc` | 발급 Lua `-4 DUP_PER_MEMBER`, 보상 Lua `0` + 기존 선점 불변 |

C6 은 **Redis 를 신뢰하지 않는다는 전제를 코드가 아니라 테스트로 증명**한다.

C9~C12 는 **읽어서는 다시 못 잡는 것들**이다. C9·C10 은 두 연산이 겹칠 때만 드러나므로 주입 지점에 래치를 걸어 결정론적으로 재현한다. C12 의 기대값에 주의한다 — 접두 비교를 고치는 목적은 그 요청을 통과시키는 것이 **아니라** 서로 다른 키를 같은 키로 오인하지 않는 것이다. 같은 회원이면 결과는 여전히 `-4` 다.

---

## 10. 측정

### 10.1 지표

| 지표 | 목표 | 근거 |
|---|---|---|
| 성공 발급 p99 | 500ms | DB 영속 포함. v2 의 진짜 성적표 |
| 거절 응답 p99 | **10ms** | Redis 1왕복 실측 p99 가 1.40ms 다. 100ms 로 두면 게이트가 70배 느려져도 안 걸려 회귀를 못 잡는다 |
| 조회 p99 (4종) | 100ms | §7 |
| gap 4축 (FINAL) | 전부 0 | 하나라도 0이 아니면 `Verdict.FAIL` |
| `ACTIVE_DB_GAP` 양수 (LIVE) | 0건 | 양수는 스큐로 설명되지 않는다 |
| `LUA_GAP` (LIVE) | 상시 0 | 즉시 CRITICAL |
| 초과 발급 · 1인 2매 | 0건 | 불변식 |
| DB unique 위반 발생 수 | 0건 | Redis 상태 이상 신호 |

### 10.2 시나리오

**발급과 조회를 모두 측정한다.** 혼합이 정식이고 발급-only 를 기준선으로 함께 돌린다 — 기준선이 없으면 조회가 발급을 얼마나 밀었는지 분리할 수 없다.

| 축 | 구성 | 산출물 |
|---|---|---|
| 램프 | 도착률 계단식 상승, **단계마다 다른 회차** | 각 버전의 p99 무릎점 = 용량. 세 곡선을 겹친다 |
| 스파이크 | 재고 1만 · 요청 2만 · 도착 1~3초 | 발표 시연 |

램프에서 회차를 바꾸는 이유는 재고 소진이다. 한 회차로 계단을 올리면 첫 단계에서 1만 장이 다 나가고 이후는 전부 매진 거절이라 발급 경로를 안 탄다. 캠페인 147개가 있으므로 단계마다 새 회차를 쓰고 §6.2 워밍업만 미리 돌린다. 회원 100만 명이라 매 요청 다른 `memberId` 를 주는 데 제약이 없다.

**조회는 v1·v2 공통 항이지만 v2 에서 오히려 나빠질 수 있다.** v1 의 `FOR UPDATE` 는 사실상 부하 제한기로 작동해 DB 쓰기 압력을 눌러 준다. v2 는 그 제한을 걷어내므로 같은 DB 를 쓰는 조회가 밀린다. **§7 캐시를 v2 와 동시에 진행해야 하는 근거다.**

**api 1대 측정의 범위를 명시한다.** v1 비판의 핵심이 "api 를 늘려도 천장이 안 변한다"였는데 v2 측정은 1대로 한다. **이 회차가 재는 것은 DB 천장이고, "게이트가 공유 자원이라 api 를 수평 확장할 수 있다"는 주장은 이 회차 범위 밖이다.** L1 Caffeine 의 single flight 를 로컬에 둔 근거(§7.2)도 N=1 전제 위에 있으므로, N 이 바뀌면 §7 이 함께 바뀐다.

**부하 생성기는 병목이 아니다(실측 — 정적 대상·로컬 브리지).**

```
HttpUser      u100 p1     4,376 rps
FastHttpUser  u100 p1    17,698 rps   ← 4.0배
FastHttpUser  u200 p4    58,892 rps
```

대상 CPU 를 4→8 로 올려도 단일 프로세스가 17.4k 에 머물러 천장이 Locust 프로세스임을 확인했다. **`FastHttpUser` 필수** — 기본 `HttpUser` 의 4,376 rps 는 서버 상한과 겹쳐 병목 구분이 안 된다. 다만 **노트북→api 무선 구간은 미검증**이다.

### 10.3 v1 대비 관찰

- **커넥션 풀 점유** — v1 은 매진 요청도 커넥션을 잡는다. v2 는 안 잡는다
- **락 대기** — V1-1 의 `FOR UPDATE` 대기 큐, V1-2 의 `occupyOne` X 락 대기가 v2 에서 사라진다. **V1-2 는 이미 락 구간이 짧아 격차가 V1-1 보다 작게 나온다**
- **Redis 명령 지연** — v2 의 새 축. 여기가 평탄한데 성공 p99 가 나쁘면 병목이 DB 에 남아 있다는 증거이고 그게 v3 의 근거다
- **`fsync/commit` 비율** — PMM 으로 관측. v2 는 2.0 근처, v3 는 0.1 이하가 예상값

### 10.4 부하 제한 — 거절하지 않는다 (D11)

스파이크 2만을 그대로 받으면 톰캣 worker 가 먼저 소진된다. 그렇다고 **DB 구간 앞에서 초과분을 429 로 떨어뜨리지 않는다.**

`HSETNX`·`DECR` 가 도착 순서대로 원자적으로 처리되므로 **Lua 를 통과한 시점에 선착순은 이미 결정됐다.** 그 뒤에 "DB 자리가 있느냐"라는 두 번째 관문을 두고 여기서 떨어뜨리면, 먼저 누른 사람이 아니라 **먼저 누르고 DB 슬롯까지 잡은 사람**이 받는다 — 선착순의 정의가 바뀐다.

> **Lua 를 통과한 요청은 전원 성공시킨다. 소화 못 한 요청은 거절이 아니라 순서대로 기다린다.**

**대가는 대기 시간이고, 상한은 톰캣 연결 수다.** 그리고 그 상한을 넘으면 429 가 아니라 **연결 실패·클라이언트 타임아웃**이 나온다 — 이쪽이 429 보다 나쁘다. 429 는 보상되어 재고가 다른 사람에게 가지만, 끊긴 요청은 보상이 실행되지 않아 **재고가 묶인다**(§4.10).

```
타임아웃 계층 — 역전되면 "기다리게 한다"가 "엉뚱한 데서 끊긴다"가 된다

k6 클라이언트 타임아웃
  > 서버 요청 전체 예산
      > 커넥션 획득 타임아웃      ← 여기가 실질 대기 상한
          > Redis 명령 타임아웃 100ms (§6.3)
```

**커넥션 풀 크기.** v1 대비 비교는 **같은 풀**로 낸다 — 버전마다 다른 풀로 재면 같은 자가 아니고, 좁은 풀이 오히려 v2 에 유리하다(v1 은 그 커넥션을 어차피 매진될 요청에도 내주고 v2 는 안 내준다). 용량 최적화 수치는 **별도 항목으로 따로** 낸다. 두 숫자를 섞어 쓰지 않는다.

**api 가 여러 대다.** 대당 당첨자는 `총 당첨 ÷ N` 이고 DB 커넥션 총합은 `대당 풀 × N` 이다. **두 숫자 모두 인스턴스당과 전체 합을 구분해서 기록한다.**

거절 경로(§4.3 의 `-1`~`-5`)는 이 대기에 참여하지 않는다. DB 를 건드리지 않으므로 애초에 자리를 필요로 하지 않는다.

**목표를 못 맞추면 무엇을 조정하나.** 구조는 확정됐으므로(D10) 지렛대는 셋뿐이다 — **인스턴스 수 · 도착 시간(램프) · 회차 재고**. 이 셋으로도 안 되면 §0.1 의 재검토 조건이 충족된 것이다.

---

## 11. 미결

| 항목 | 비고 |
|---|---|
| 노트북→api 무선 구간 실측 | 회차 전 체크. 설계 입력은 아니다 |
| 램프 단계 수치 | 위 실측 후 확정 |
| 사용취소가 재고 복원 대상인지 | `CouponStateMachine` 전이 규칙과 대조 |
| Kafka producer `max.block.ms` | 브로커 사망 + 버퍼 만석 시 `send()` 블로킹 |
| v1 회차에서 Kafka 를 띄우는지 | "v1 은 순수 Java+MySQL" 의 실제 범위 |
| `idx_issuances_member_issued` 재추가 | V12 가 **의도적으로** 지웠고 나중에 붙일 예정. 그때까지 내 쿠폰함은 filesort |
| ~~api 를 N대로 늘릴 때~~ **이미 N대다** | L2 미스 분산 락·L1 evict 는 §0.1 에서 반려. **낡음의 상한은 TTL 이 보장**하고 무효화는 최적화로만 다룬다 |
| **Prometheus 가 단일 타깃** | `targets: ['api:9090']` 하나라 스크레이프마다 다른 인스턴스를 긁는다. **인스턴스별로 나누지 않으면 §10 수치를 못 쓴다** |
| **설정이 이 문서와 어긋난다** | Redis 명령 타임아웃 실제 `500ms`(문서 100ms) · 커넥션 풀 **yml 미설정** · 톰캣 스레드 실제 `15`. **측정 전에 정렬한다** |
| **재구성 동시 실행 차단** | api 가 여러 대라 §6.2·§9.7 이 겹쳐 돌 수 있다. batch 단독 소유 또는 락(§6.2) |
| **V1-1 · V1-2 를 둘 다 측정하는가** | 그러면 사다리가 4단. PRD 의 "v1 = 비관적 락" 서술을 V1-1 로 좁혀야 한다(§8.2) |
| **V1-2 를 CY-5 로 가져오기** | `1b5c7fa5` 는 `main` 기반이라 CY-5 pull 로는 안 따라온다. cherry-pick 필요 |
| **잠금 순서 역전(G)이 실제 데드락을 만드는가** | 부하 회차에서 `LATEST DETECTED DEADLOCK` 확인 |
