# 03 · 완료 CAS · 보상 CAS

영속화가 끝나면 `D` 로 올리고, 실패하면 되돌린다.
**두 연산 모두 현재 값이 자기 선점일 때만 수행한다.**

무조건 `HSET` 하거나 접두만 맞으면 `HDEL` 하면, 되돌리기와 완료가 겹칠 때
**지워진 자리가 되살아나고 재고가 한 장 늘어난다.**

> 파손 값 회수는 **`13`**, 두 스크립트가 공유하는 인자 가드는 **`12`** 에 있다.

## 완료 승격

```lua
-- KEYS[1]=issued  ARGV[1]=memberId ARGV[2]=requestToken
if #ARGV < 2 then return -10 end
if #ARGV[1] == 0 or #ARGV[2] == 0 or string.find(ARGV[2], '|', 1, true) then return -10 end
local stored = redis.call('HGET', KEYS[1], ARGV[1])
if stored == false then return -1 end                      -- 사라졌다. 보상과 겹쳤다
local st, ms, tk, key = string.match(stored, '^([PD])|(%d+)|([^|]+)|(.+)$')
if st == nil or #ms > 13 then return -3 end                -- 값 형식 파손
if tk ~= ARGV[2] then return -2 end                        -- 남의 선점. 건드리지 않는다
if st == 'D' then return 0 end                             -- 이미 DONE. 재시도끼리 겹친 것
redis.call('HSET', KEYS[1], ARGV[1], 'D|' .. ms .. '|' .. tk .. '|' .. key)
return 1
```

## 보상

```lua
-- KEYS[1]=stock KEYS[2]=issued KEYS[3]=issued_ever
-- ARGV[1]=memberId ARGV[2]=requestToken
if #ARGV < 2 then return -10 end
if #ARGV[1] == 0 or #ARGV[2] == 0 or string.find(ARGV[2], '|', 1, true) then return -10 end
local stored = redis.call('HGET', KEYS[2], ARGV[1])
if stored == false then return 0 end                       -- 이미 없다
local st, ms, tk, key = string.match(stored, '^([PD])|(%d+)|([^|]+)|(.+)$')
if st == nil or #ms > 13 then return -3 end                -- 값 형식 파손. 네 필드를 전부 본다
if tk ~= ARGV[2] then return 0 end                         -- 내 선점이 아니면 아무것도 안 한다
if st ~= 'P' then return -1 end                            -- 이미 DONE 이면 보상 금지. 경보
if not readableCounters(KEYS[1], KEYS[3]) then return -11 end  -- 되돌리기 전에 본다(12)
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('INCR', KEYS[1])
redis.call('DECR', KEYS[3])
return 1
```

## 반환 코드

| | `1` | `0` | 음수 |
|---|---|---|---|
| 완료 | 승격함 | 이미 `D` — 정상 | `-1` 사라짐 · `-2` 남의 선점 · `-3` 파손 · `-10` 인자 이상 |
| 보상 | 되돌림 | 내 것이 아님 — 정상 | `-1` 이미 `D` · `-3` 파손 · `-10` 인자 이상 |

**쓸 수 없는 토큰은 비교하지 말고 `-10` 으로 막는다.** 저장된 토큰은 `([^|]+)` 라
빈 값이나 `|` 가 든 값과 **같아질 수 없다.** 그런 인자를 그냥 비교하면 보상은 언제나
`0`(내 것이 아님 = 정상)을 돌려주고 — **삼켜지는 실패다.** 카운터도 안 오르는데
선점 때 깎인 재고는 영구히 잠긴다. 완료는 `-2`(남의 선점)로 나가 경보는 뜨지만,
원인이 "경합" 으로 오진된다.

**음수는 전부 이상 신호다.** 카운터를 올리고 경보한다.
**삼키면 이 문서 전체가 아무 의미가 없다.**

## 승격은 상태 한 글자만 바꾼다

선점시각을 완료시각으로 **덮지 않는다.** 완료 시각의 원본은 DB(`issuances`)이고,
**"언제 선점됐나" 는 Redis 에만 있는 정보다.** 덮으면 `claimedAtEpochMillis` 라는 이름이
`D` 상태에서 거짓이 되고, 재구성·감사에서 선점 시각을 쓸 수 없다.

그래서 **완료시각 인자 자체가 없다.** 인자가 없으면 그것을 잘못 넘겨 생기는 결함도 없다 —
순서를 틀린 어댑터가 파손 값을 `D` 로 올리고 `1` 을 돌려주던 경로가 **가드가 아니라
구조로** 사라진다. 남은 인자는 토큰 하나이고, 그건 쓰기 전에 본다.

`stalePendingCount` 는 `P` 만 세므로(05) 이 변경의 영향을 받지 않는다.
관제 지표 중 이 필드의 **값**을 읽는 것은 아직 없다.

## `issued_ever` 에 하한을 두지 않는다

`DECR` 이 음수까지 내려간다. **일부러 그렇게 둔다.**
음수가 되려면 이미 다른 축이 깨진 뒤인데, 0에서 잘라 두면 `LUA_GAP` 이 그 사실을 못 본다 —
**감지 장치를 스스로 무력화하는 것**이라 01 이 `RECLAIMED` 상태를 금지한 것과 같은 이유다.
음수는 그대로 노출되어 재동기화 대상이 된다.

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
| 파손 감지(`-8` · `-3`) | **회수하지 않는다.** `valueCorrupt` 를 올리고 경보(S5) |
| 재구성 · 재동기화 | 그 절차 안에서 파손 회수(S8). 게이트가 닫힌 상태라 경합이 없다 |
| 영속화 성공 | **완료 CAS** → 201 |
| INSERT 예외 · **재조회 결과 행 없음** | **보상** → 5xx |
| INSERT 예외 · 재조회 결과 행 있음 | **완료 CAS** 후 그 결과로 응답. 보상하지 않는다 |
| INSERT 예외 · **재조회 자체 실패** | **아무것도 하지 않는다.** `P` 로 남긴다(05 참조) |

**불확실하면 보상하지 않는다.** 보상 누락은 과소(안전), 잘못된 보상은 초과 방향(위험)이다.

## S5 · S7 · S8 인계

- **S5** — `-8`/`-3` 은 회수를 부르는 자리가 아니라 **경보하는 자리**다.
- **S7** — 완료 CAS 는 선점시각을 **보존한다.** 그래서 `D` 의 나이도 읽을 수 있지만 그건
  "선점된 지 얼마" 이지 "완료된 지 얼마" 가 아니다. `D` 를 stale 로 세지 않는다(05).
- **S8** — 재구성 3번(`issued` 재작성) 앞에 파손 회수를 넣는다(`13`).
