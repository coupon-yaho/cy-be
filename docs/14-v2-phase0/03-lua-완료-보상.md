# 03 · 완료 CAS · 보상 CAS

영속화가 끝나면 `D` 로 올리고, 실패하면 되돌린다.
**두 연산 모두 현재 값이 자기 선점일 때만 수행한다.**

무조건 `HSET` 하거나 접두만 맞으면 `HDEL` 하면, 되돌리기와 완료가 겹칠 때
**지워진 자리가 되살아나고 재고가 한 장 늘어난다.**

## 완료 승격

```lua
-- KEYS[1]=issued  ARGV[1]=memberId ARGV[2]=requestToken ARGV[3]=nowMillis
local stored = redis.call('HGET', KEYS[1], ARGV[1])
if stored == false then return -1 end                      -- 사라졌다. 보상과 겹쳤다
local st, ms, tk, key = string.match(stored, '^([PD])|(%d+)|([^|]+)|(.*)$')
if st == nil then return -3 end                            -- 값 형식 파손
if tk ~= ARGV[2] then return -2 end                        -- 남의 선점. 건드리지 않는다
if st == 'D' then return 0 end                             -- 이미 DONE. 재시도끼리 겹친 것
redis.call('HSET', KEYS[1], ARGV[1], 'D|' .. ARGV[3] .. '|' .. tk .. '|' .. key)
return 1
```

## 보상

```lua
-- KEYS[1]=stock KEYS[2]=issued KEYS[3]=issued_ever
-- ARGV[1]=memberId ARGV[2]=requestToken
local stored = redis.call('HGET', KEYS[2], ARGV[1])
if stored == false then return 0 end                       -- 이미 없다
local st, ms, tk = string.match(stored, '^([PD])|(%d+)|([^|]+)|')
if st == nil then return -3 end                            -- 값 형식 파손
if tk ~= ARGV[2] then return 0 end                         -- 내 선점이 아니면 아무것도 안 한다
if st ~= 'P' then return -1 end                            -- 이미 DONE 이면 보상 금지. 경보
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('INCR', KEYS[1])
redis.call('DECR', KEYS[3])
return 1
```

## 반환 코드

| | `1` | `0` | 음수 |
|---|---|---|---|
| 완료 | 승격함 | 이미 `D` — 정상 | `-1` 사라짐 · `-2` 남의 선점 · `-3` 파손 |
| 보상 | 되돌림 | 내 것이 아님 — 정상 | `-1` 이미 `D` · `-3` 파손 |

**음수는 전부 이상 신호다.** 카운터를 올리고 경보한다.
**삼키면 이 문서 전체가 아무 의미가 없다.**

**S2 인계** — 완료·보상도 상태를 `[PD]`로만 캡처한다. `X|1|t|k`는 두 스크립트
모두 `-3`이어야 하며, Java codec·선점 Lua와 같은 파손 기준을 계약 테스트로 묶는다.

## 되돌리는 범위가 다르다

| | stock | issued_ever | issued Hash |
|---|---|---|---|
| **보상** | `INCR` | `DECR` | `HDEL` |
| **취소·사용취소·만료** | `INCR` | — | — |

보상은 "발급이 없었던 일"이라 셋을 다 되돌린다.
취소는 "발급은 있었으나 재고는 반납"이라 `stock` 만 되돌린다.
**이 비대칭을 틀리면 정합성 축 두 개가 조용히 어긋난다.**

## 토큰 비교가 방어선이다

멱등키만으로는 부족하다 — **같은 키의 서로 다른 시도를 구별하지 못한다.**
토큰이 없으면 늦게 도착한 보상이나 중복 실행이 재고를 두 번 늘려 초과 발급이 난다.

## 호출 지점

| 상황 | 호출 |
|---|---|
| 영속화 성공 | **완료 CAS** → 201 |
| INSERT 예외 · **재조회 결과 행 없음** | **보상** → 5xx |
| INSERT 예외 · 재조회 결과 행 있음 | **완료 CAS** 후 그 결과로 응답. 보상하지 않는다 |
| INSERT 예외 · **재조회 자체 실패** | **아무것도 하지 않는다.** `P` 로 남긴다(05 참조) |

**불확실하면 보상하지 않는다.** 보상 누락은 과소(안전), 잘못된 보상은 초과 방향(위험)이다.
