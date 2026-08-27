# 02 · 발급 선점 스크립트

정책 검증 · 중복 판정 · 재고 차감을 **한 번의 원자 실행**으로 끝낸다.
거절이면 DB 를 건드리지 않는다 — v2 의 가장 확실한 이득이 여기서 나온다.

## 스크립트

```lua
-- KEYS[1]=stock KEYS[2]=issued KEYS[3]=meta KEYS[4]=issued_ever
-- ARGV[1]=memberId ARGV[2]=nowMillis ARGV[3]=gradeBit
-- ARGV[4]=idempotencyKey ARGV[5]=requestToken

local meta = redis.call('HMGET', KEYS[3], 'status','openAt','closeAt','gradeMask')
if not meta[1] then return {-9} end                        -- NOT_READY: 재구성 중
local now = tonumber(ARGV[2])
if meta[1] ~= 'OPEN' or now >= tonumber(meta[3]) then return {-1} end
if now < tonumber(meta[2]) then return {-2} end
if bit.band(tonumber(meta[4]), tonumber(ARGV[3])) == 0 then return {-3} end

-- 값 형식: <상태>|<선점시각>|<요청토큰>|<멱등키>   (01 참조)
local claimed = redis.call('HSETNX', KEYS[2], ARGV[1],
        'P|' .. ARGV[2] .. '|' .. ARGV[5] .. '|' .. ARGV[4])
if claimed == 0 then
    local stored = redis.call('HGET', KEYS[2], ARGV[1])
    local st, ms, tk, key = string.match(stored, '^([PD])|(%d+)|([^|]+)|(.+)$')
    if st == nil or #ms > 13 then return {-8} end          -- 값 형식 파손. 경보
    if key ~= ARGV[4] then return {-4} end                 -- DUP_PER_MEMBER
    if st == 'D' then return {-6} end                      -- REPLAY_DONE
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
| `-8` | **값 형식 파손** | 500 + 경보. 정상 운영에서 0 |
| `-9` | 게이트 미준비(재구성 중) | 503 + `Retry-After` |

## 지켜야 하는 것

**세 쓰기가 반드시 같은 스크립트 안에 있어야 한다** — `DECR stock` · `INCR issued_ever` ·
`HSETNX issued`. 하나라도 밖으로 나가면 `LUA_GAP` 이 즉시 CRITICAL 이다.

**중복 판정을 재고 확인보다 먼저 한다.** 반대로 두면 "재고를 깎았는데 중복이라 되돌린다"가 되어
그 찰나에 다른 요청이 매진으로 거절될 수 있다. `SOLD_OUT` 일 때만 `HDEL` 하며,
`HSETNX` 반환값 1이 "방금 내가 만든 field" 임을 보장한다.

**키 비교는 전체 일치다.** 멱등키는 클라이언트가 정하므로 `abc` 와 `abcdef` 같은 접두 충돌을
유도할 수 있고, 접두 비교로 두면 03 의 보상이 **남의 선점을 되돌린다** — 초과 발급 방향이다.

**S2 인계** — 상태 캡처는 `%a`가 아니라 `[PD]`다. `X|1|t|k`를 파손 `-8`로 보는
Lua 계약 테스트를 넣어 Java codec과 판정이 다시 갈라지지 않게 한다.

**O(1) 만 쓴다.** 루프 금지. `HGETALL`·`HKEYS`·`KEYS` 금지(§3.3).
