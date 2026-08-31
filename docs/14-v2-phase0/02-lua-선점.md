# 02 · 발급 선점 스크립트

정책 검증 · 중복 판정 · 재고 차감을 **한 번의 원자 실행**으로 끝낸다.
거절이면 DB 를 건드리지 않는다 — v2 의 가장 확실한 이득이 여기서 나온다.

> 인자 가드 · 시각 원본 · `meta` 필드 계약은 **`12`** 로 뺐다. 5종이 공유하는 규약이라
> 여기 두면 스크립트마다 같은 내용이 반복된다.

## 스크립트

```lua
-- 인자·카운터 판정(isCanonicalInt 포함)은 12. 여기서는 흐름만 본다.
-- KEYS[1]=stock KEYS[2]=issued KEYS[3]=meta KEYS[4]=issued_ever
-- ARGV[1]=memberId ARGV[2]=gradeBit ARGV[3]=idempotencyKey ARGV[4]=requestToken

if #ARGV < 4 then return {-10} end
if #ARGV[1] == 0 or #ARGV[3] == 0 or #ARGV[4] == 0
        or string.find(ARGV[4], '|', 1, true)
        or string.match(ARGV[2], '^%d+$') == nil
        or tonumber(ARGV[2]) > 2147483647 then
    return {-10}                                           -- 인자 이상. 쓰기 전에 막는다
end

local meta = redis.call('HMGET', KEYS[3],
        'status','openAt','closeAt','gradeMask','totalQuantity')
if not meta[1] or #meta[1] == 0 or not meta[2] or not meta[3]
        or not meta[4] or not meta[5] then
    return {-9}
end
local openAt, closeAt = tonumber(meta[2]), tonumber(meta[3])
local mask, total = tonumber(meta[4]), tonumber(meta[5])
if openAt == nil or closeAt == nil or mask == nil or total == nil then return {-9} end
if mask < 0 or mask > 2147483647 or mask ~= math.floor(mask) then return {-9} end

local clock = redis.call('TIME')                           -- 시각의 원본은 Redis 하나다
local seconds, micros = tonumber(clock[1]), tonumber(clock[2])
local now = seconds * 1000 + math.floor(micros / 1000)
local nowText = string.format('%d%03d', seconds, math.floor(micros / 1000))
local reject = nil
if meta[1] ~= 'OPEN' or now >= closeAt then reject = -1
elseif now < openAt then reject = -2
elseif bit.band(mask, tonumber(ARGV[2])) == 0 then reject = -3 end
if reject ~= nil then                                      -- 거절 전에 멱등부터 본다
    local stored = redis.call('HGET', KEYS[2], ARGV[1])
    if stored == false then return {reject} end
    local st, ms, tk, key = string.match(stored, '^([PD])|(%d+)|([^|]+)|(.+)$')
    if st == nil or #ms > 13 then return {-8} end
    if key ~= ARGV[3] then return {reject} end
    if st == 'D' then return {-6} end
    return {-7}
end

-- 값 형식: <상태>|<선점시각>|<요청토큰>|<멱등키>   (01 참조)
local claimed = redis.call('HSETNX', KEYS[2], ARGV[1],
        'P|' .. nowText .. '|' .. ARGV[4] .. '|' .. ARGV[3])
if claimed == 0 then
    local stored = redis.call('HGET', KEYS[2], ARGV[1])
    local st, ms, tk, key = string.match(stored, '^([PD])|(%d+)|([^|]+)|(.+)$')
    if st == nil or #ms > 13 then return {-8} end          -- 값 형식 파손. 경보
    if key ~= ARGV[3] then return {-4} end                 -- DUP_PER_MEMBER
    if st == 'D' then return {-6} end                      -- REPLAY_DONE
    return {-7}                                            -- REPLAY_PENDING
end

-- Redis 의 DECR·INCR 은 canonical 정수 문자열만 받는다(12). 늦게 보면 고아가 남는다
local raw = redis.pcall('GET', KEYS[1])
local rawEver = redis.pcall('GET', KEYS[4])
if not isCanonicalInt(raw, true)
        or (rawEver ~= false and not isCanonicalInt(rawEver, true)) then
    redis.call('HDEL', KEYS[2], ARGV[1])                   -- 방금 잡은 선점만 되돌린다
    return {-11}                                           -- 카운터를 못 읽는다. 매진이 아니다
end
local left = tonumber(raw)
if left <= 0 then
    redis.call('HDEL', KEYS[2], ARGV[1])
    return {-5}                                            -- SOLD_OUT
end

redis.call('DECR', KEYS[1])
redis.call('INCR', KEYS[4])
return {0, left - 1}
```

## 반환 코드

| 코드 | 뜻 | 다음 동작 |
|---|---|---|
| `0` | 선점 성공 | 영속화로 진행. 두 번째 원소가 잔여 재고 |
| `-1` | 마감 | 거절 |
| `-2` | 미오픈 | 거절 |
| `-3` | 등급 미달 | 거절 |
| `-4` | 이 회원이 **이미 받음** | 거절 |
| `-5` | 매진 | 거절 |
| `-6` | 같은 키 재시도 · **완료됨** | **최초 응답 재사용**(04 참조) |
| `-7` | 같은 키 재시도 · 처리 중 | 409 + `Retry-After` |
| `-8` | **값 형식 파손** | 500 + 경보. 정상 운영에서 0. 회수는 13 |
| `-9` | 게이트 미준비(재구성 중 · meta 부분 상태) | 503 + `Retry-After` |
| `-10` | **인자 이상** | 500 + 경보. 호출부 버그. 정상 운영에서 0 |
| `-11` | **카운터를 못 읽음**(`stock`·`issued_ever` 부재 · canonical 정수 아님 · 자료형) | 503 + 경보. 운영 개입. 정상 운영에서 0 |

## 멱등 판정이 게이트보다 먼저다

마감 1ms 뒤에 도착한 **재시도**를 `-1`(마감)로 돌려주면, DB 에는 쿠폰이 있는데 클라이언트는
실패 응답을 받는다. **그건 멱등이 아니라 고장이다**(04 의 `-4`/`-6` 논리가 그대로 적용된다).

그래서 게이트 거절이 확정돼도 **같은 멱등키의 기존 선점이 있으면 그쪽 답을 준다.**

```
같은 키 + D   → -6   (마감이어도 최초 응답 재사용)
같은 키 + P   → -7
다른 키       → 게이트 코드 그대로 (-1 · -2 · -3)
파손          → -8   (경보가 게이트에 가려지면 안 된다)
```

**왕복은 여전히 1회다.** 한 스크립트 안에서 `HGET` 이 한 번 더 도는 것뿐이고,
그것도 **거절 경로에서만**이다. 매진(`-5`)은 게이트 뒤라 영향이 없다.
멱등 조회를 아예 게이트 앞으로 빼면 성공 경로까지 `HGET` 이 붙으므로 그렇게 하지 않았다.

## 재고를 못 읽는 것은 매진이 아니다

`left == nil` 과 `left <= 0` 을 한 분기로 두면 `stock` 키가 사라졌을 때 **재고가 8,000장
남아 있어도 전량 매진으로 종단 거절**된다. 클라이언트는 재시도조차 하지 않는다.

`-9` 와도 합치지 않는다. **대응이 다르기 때문이다.**

```
-9   재구성 창. 정상적인 상태이고 기다리면 풀린다
-11  키 부재·비숫자. 사람이 봐야 풀린다
```

합쳐 두면 재구성 창의 정상적인 503 에 진짜 사고가 묻힌다. `meta` 는 재구성에서 **가장
마지막에 쓰이므로**, `meta` 가 `OPEN` 인데 `stock` 이 없다는 것은 그 자체로 이상 상태다.

**자료형 오류도 canonical 정수가 아닌 값도 `-11` 이다.** `HSETNX` 로 field 를 만든 **뒤에** 카운터를 읽으므로,
`DECR` 이 터지면 롤백이 실행되지 않아 **DECR 없는 고아 `P` 가 남는다.** 그 값은 형식상
멀쩡해서 파손 회수(`13`)로도 못 지우고, 그 회원은 영구히 `-7` 로 막힌다.
`issued_ever` 도 **`INCR` 앞에서** 같이 본다 — `DECR` 만 성공하면 `PERSIST_GAP` 이 어긋난다.
판정 기준은 12 에 있다.

`meta` 쪽 `HMGET` 은 감싸지 않는다. 그건 **쓰기 전**이라 터져도 상태가 안 남는다.

## 지켜야 하는 것

**세 쓰기가 반드시 같은 스크립트 안에 있어야 한다** — `DECR stock` · `INCR issued_ever` ·
`HSETNX issued`. 하나라도 밖으로 나가면 `LUA_GAP` 이 즉시 CRITICAL 이다.

**중복 판정을 재고 확인보다 먼저 한다.** 반대로 두면 "재고를 깎았는데 중복이라 되돌린다"가 되어
그 찰나에 다른 요청이 매진으로 거절될 수 있다. `SOLD_OUT` 일 때만 `HDEL` 하며,
`HSETNX` 반환값 1이 "방금 내가 만든 field" 임을 보장한다.

**키 비교는 전체 일치다.** 멱등키는 클라이언트가 정하므로 `abc` 와 `abcdef` 같은 접두 충돌을
유도할 수 있고, 접두 비교로 두면 03 의 보상이 **남의 선점을 되돌린다** — 초과 발급 방향이다.

**O(1) 만 쓴다.** 루프 금지. `HGETALL`·`HKEYS`·`KEYS` 금지(§3.3).
