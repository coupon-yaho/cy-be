package com.kafkick.infra.redis.coupon.v2;

import java.util.List;

import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * v2 발급의 Lua 5종 — 선점 · 완료 CAS · 보상 CAS · 배치 복원 · 파손 회수.
 * 본문은 {@code docs/14-v2-phase0/02·03·06·13} 의 스크립트와 같고, 5종이 공유하는
 * 인자·카운터 규약은 {@code 12} 에 있다.
 *
 * <p><b>선점의 세 쓰기는 반드시 한 스크립트 안에 있어야 한다</b> — {@code DECR stock} ·
 * {@code INCR issued_ever} · {@code HSETNX issued}. 하나라도 밖으로 나가면 {@code LUA_GAP} 이
 * 즉시 CRITICAL 이다.
 *
 * <p>값 파싱은 접두가 아니라 4필드 전체 분해다(01). 상태 문자는 {@code %a} 가 아니라
 * {@code [PD]} 로, 선점시각은 13자리까지로 제한한다 — Java codec 과 판정이 갈리면
 * 두 정합성 축이 영구히 어긋난다.
 *
 * <p>O(1) 만 쓴다. 루프·{@code HGETALL}·{@code HKEYS}·{@code KEYS} 는 없다.
 *
 * <p><b>{@code isCanonicalInt} 는 Redis 의 {@code string2ll} 보다 일부러 좁다.</b> 형식은 같지만
 * ({@code 0} 또는 {@code -?[1-9]%d*}) <b>자릿수를 15로 묶는다.</b> 양쪽으로 실패 모드가 있다.
 *
 * <ul>
 *   <li><b>넓으면 쓰기가 중간에서 터진다</b> — {@code '007'}·{@code '-0'} 은 {@code tonumber}
 *       도 {@code '^-?%d+$'} 도 통과시키지만 {@code DECR} 이 거부한다. Lua 는 원자적이어도
 *       이미 적용된 쓰기를 되돌리지 않아 짝 없는 {@code P} 가 남는다.</li>
 *   <li><b>너무 넓어도 상한 검사가 조용히 무력화된다</b> — Lua 5.1 의 수는 double 이라
 *       2^53 위에서는 {@code a + 1 > a} 가 {@code false} 다
 *       ({@code tonumber('9223372036854775807')} → {@code 9.2233720368548e+18}).
 *       {@code left + n > total} 이 바로 그 비교다.</li>
 * </ul>
 *
 * <p>15자리는 2^53(≈9.0e15) 아래라 정수 연산이 정확하다. 재고·누적 발급수가 10^15 를
 * 넘을 일은 없으므로 <b>int64 전체로 넓히지 않는다.</b>
 */
public final class IssuanceScripts {

    /**
     * 선점. KEYS = stock, issued, meta, issued_ever /
     * ARGV = memberId, gradeBit, idempotencyKey, requestToken.
     *
     * <p><b>시각의 원본은 Redis 하나다</b>({@code TIME}). 호출 인스턴스가 자기 시계를 넘기면
     * api 가 여러 대인 만큼 마감 경계의 답이 갈리고, 시계가 앞선 인스턴스가 남긴 선점은
     * 나이가 음수라 {@code stalePendingCount} 가 영원히 못 잡는다.
     * 반환은 {@code {코드}} 이고 성공일 때만 {@code {0, 잔여재고}} 다.
     */
    @SuppressWarnings("rawtypes")
    public static final RedisScript<List> CLAIM = new DefaultRedisScript<>("""
            local function isCanonicalInt(s, signed)
                if s == false or type(s) == 'table' or #s > 15 then return false end
                if s == '0' then return true end
                if signed then return string.match(s, '^-?[1-9]%d*$') ~= nil end
                return string.match(s, '^[1-9]%d*$') ~= nil
            end
            if #ARGV < 4 then return {-10} end
            if #ARGV[1] == 0 or #ARGV[3] == 0 or #ARGV[4] == 0
                    or string.find(ARGV[4], '|', 1, true)
                    or string.match(ARGV[2], '^%d+$') == nil
                    or tonumber(ARGV[2]) > 2147483647 then
                return {-10}
            end

            local meta = redis.call('HMGET', KEYS[3],
                    'status','openAt','closeAt','gradeMask','totalQuantity')
            if not meta[1] or #meta[1] == 0 or not meta[2] or not meta[3]
                    or not meta[4] or not meta[5] then
                return {-9}
            end
            local openAt, closeAt = tonumber(meta[2]), tonumber(meta[3])
            local mask, total = tonumber(meta[4]), tonumber(meta[5])
            if openAt == nil or closeAt == nil or mask == nil then return {-9} end
            if not isCanonicalInt(meta[5], false) or total == nil then return {-9} end
            if mask < 0 or mask > 2147483647 or mask ~= math.floor(mask) then return {-9} end

            local clock = redis.call('TIME')
            local seconds, micros = tonumber(clock[1]), tonumber(clock[2])
            local now = seconds * 1000 + math.floor(micros / 1000)
            local nowText = string.format('%d%03d', seconds, math.floor(micros / 1000))
            local reject = nil
            if meta[1] ~= 'OPEN' or now >= closeAt then reject = -1
            elseif now < openAt then reject = -2
            elseif bit.band(mask, tonumber(ARGV[2])) == 0 then reject = -3 end
            if reject ~= nil then
                local stored = redis.call('HGET', KEYS[2], ARGV[1])
                if stored == false then return {reject} end
                local st, ms, tk, key = string.match(stored, '^([PD])|(%d+)|([^|]+)|(.+)$')
                if st == nil or #ms > 13 then return {-8} end
                if key ~= ARGV[3] then return {reject} end
                if st == 'D' then return {-6} end
                return {-7}
            end

            local claimed = redis.call('HSETNX', KEYS[2], ARGV[1],
                    'P|' .. nowText .. '|' .. ARGV[4] .. '|' .. ARGV[3])
            if claimed == 0 then
                local stored = redis.call('HGET', KEYS[2], ARGV[1])
                local st, ms, tk, key = string.match(stored, '^([PD])|(%d+)|([^|]+)|(.+)$')
                if st == nil or #ms > 13 then return {-8} end
                if key ~= ARGV[3] then return {-4} end
                if st == 'D' then return {-6} end
                return {-7}
            end

            -- Redis 의 DECR·INCR 은 canonical 정수 문자열만 받는다. '1e1' 은 Lua 로는 정수지만
            -- DECR 이 터지고, 그때 HSETNX 는 이미 적용돼 있어 고아 P 가 남는다.
            local raw = redis.pcall('GET', KEYS[1])
            local rawEver = redis.pcall('GET', KEYS[4])                -- 키 부재는 예열이라 정상이다
            -- 허용 집합은 INCR 에 대해 닫혀 있어야 한다. 15자리 9만 있는 값을 통과시키면
            -- 우리가 쓴 16자리를 다음 호출의 같은 가드가 파손으로 막는다.
            if not isCanonicalInt(raw, true)
                    or (rawEver ~= false and (not isCanonicalInt(rawEver, true)
                        or string.match(rawEver, '^9+$') ~= nil and #rawEver == 15)) then
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
            """, List.class);

    /**
     * 완료 승격. KEYS = issued / ARGV = memberId, requestToken.
     * 현재 값이 <b>자기 선점일 때만</b> 올린다.
     *
     * <p><b>상태 한 글자만 바꾼다.</b> 선점시각은 그대로 둔다 — 완료시각의 원본은 DB
     * ({@code issuances})이고, "언제 선점됐나" 는 Redis 에만 있다. 그래서 <b>완료시각 인자
     * 자체가 없다</b>: 인자가 없으면 그것을 잘못 넘겨 생기는 결함도 없다.
     *
     * <p>남은 인자인 토큰은 <b>쓰기 전에</b> 본다. 저장된 토큰은 {@code ([^|]+)} 라 빈 값이나
     * {@code '|'} 가 든 값과 <b>같아질 수 없어서</b>, 그런 인자는 비교가 아니라 버그다.
     */
    public static final RedisScript<Long> COMPLETE = new DefaultRedisScript<>("""
            if #ARGV < 2 then return -10 end
            if #ARGV[1] == 0 or #ARGV[2] == 0 or string.find(ARGV[2], '|', 1, true) then return -10 end
            local stored = redis.call('HGET', KEYS[1], ARGV[1])
            if stored == false then return -1 end
            local st, ms, tk, key = string.match(stored, '^([PD])|(%d+)|([^|]+)|(.+)$')
            if st == nil or #ms > 13 then return -3 end
            if tk ~= ARGV[2] then return -2 end
            if st == 'D' then return 0 end
            redis.call('HSET', KEYS[1], ARGV[1], 'D|' .. ms .. '|' .. tk .. '|' .. key)
            return 1
            """, Long.class);

    /**
     * 보상. KEYS = stock, issued, issued_ever / ARGV = memberId, requestToken.
     *
     * <p><b>쓸 수 없는 토큰은 {@code -10} 이다.</b> 빈 토큰을 그냥 비교하면 언제나
     * {@code 0}(내 것이 아님 = 정상)이 나가고, 선점 때 깎인 재고가 조용히 영구 잠긴다 —
     * 삼켜지는 실패라 카운터도 안 오른다.
     * 보상은 "발급이 없었던 일" 이라 셋을 다 되돌린다 — 취소·만료의 복원과 범위가 다르다(03).
     */
    public static final RedisScript<Long> COMPENSATE = new DefaultRedisScript<>("""
            local function isCanonicalInt(s, signed)
                if s == false or type(s) == 'table' or #s > 15 then return false end
                if s == '0' then return true end
                if signed then return string.match(s, '^-?[1-9]%d*$') ~= nil end
                return string.match(s, '^[1-9]%d*$') ~= nil
            end
            local function readableCounters(stockKey, everKey)
                if not isCanonicalInt(redis.pcall('GET', stockKey), true) then return false end
                local rawEver = redis.pcall('GET', everKey)
                if rawEver == false then return true end               -- 키 부재는 예열이다
                return isCanonicalInt(rawEver, true)
            end
            if #ARGV < 2 then return -10 end
            if #ARGV[1] == 0 or #ARGV[2] == 0 or string.find(ARGV[2], '|', 1, true) then return -10 end
            local stored = redis.call('HGET', KEYS[2], ARGV[1])
            if stored == false then return 0 end
            local st, ms, tk, key = string.match(stored, '^([PD])|(%d+)|([^|]+)|(.+)$')
            if st == nil or #ms > 13 then return -3 end
            if tk ~= ARGV[2] then return 0 end
            if st ~= 'P' then return -1 end
            if not readableCounters(KEYS[1], KEYS[3]) then return -11 end
            redis.call('HDEL', KEYS[2], ARGV[1])
            redis.call('INCR', KEYS[1])
            redis.call('DECR', KEYS[3])
            return 1
            """, Long.class);

    /**
     * 만료 배치의 재고 복원. KEYS = stock, meta / ARGV = 건수.
     * 상한 검사를 <b>같은 원자 실행 안에서</b> 한다 — {@code INCRBY} 를 그냥 쏘면 상한을
     * 한 번에 뛰어넘고, 그 순간 초과 발급이 확정된다(06).
     */
    public static final RedisScript<Long> RESTORE = new DefaultRedisScript<>("""
            local function isCanonicalInt(s, signed)
                if s == false or type(s) == 'table' or #s > 15 then return false end
                if s == '0' then return true end
                if signed then return string.match(s, '^-?[1-9]%d*$') ~= nil end
                return string.match(s, '^[1-9]%d*$') ~= nil
            end
            if #ARGV < 1 then return -3 end
            local meta = redis.call('HMGET', KEYS[2],
                    'status','openAt','closeAt','gradeMask','totalQuantity')
            if not meta[1] or #meta[1] == 0 or not meta[2] or not meta[3]
                    or not meta[4] or not isCanonicalInt(meta[5], false) then
                return -1                                              -- 부분 상태는 재구성 창이다
            end
            local total = tonumber(meta[5])
            local raw = redis.pcall('GET', KEYS[1])
            if not isCanonicalInt(raw, true) then return -11 end
            local left = tonumber(raw)
            if not isCanonicalInt(ARGV[1], false) then return -3 end
            local n = tonumber(ARGV[1])
            if n == nil or n <= 0 or n ~= math.floor(n) then return -3 end
            if left + n > total then return -2 end
            redis.call('INCRBY', KEYS[1], n)
            return 1
            """, Long.class);

    /**
     * 파손 값 회수. KEYS = stock, issued, issued_ever /
     * ARGV = memberId, 복원여부({@code '1'}/{@code '0'}), 총재고.
     *
     * <p>총재고를 <b>{@code meta} 가 아니라 인자로</b> 받는다 — 이 스크립트는 재구성 절차
     * 안에서만 도는데, 그때 {@code meta} 는 1번 단계에서 이미 지워져 있다. 호출부(S8)는
     * DB 에서 총재고를 알고 있다.
     *
     * <p>파손 field 는 선점 {@code -8} · 완료·보상 {@code -3} 으로 <b>읽히기만 하고 아무도
     * 지우지 않는다</b>. 선점 시점에 {@code stock} 은 이미 깎였으므로 그 자리는 영구히 잠기고
     * 재고가 한 장 증발한다 — 만료로도 배치 복원으로도 안 풀린다.
     *
     * <p><b>되돌리는 범위는 호출부가 정한다.</b> 파손된 값에는 원래 상태가 안 남아 `P` 였는지
     * `D` 였는지 스크립트 단독으로는 못 푼다. `D` 였던 값에 재고를 되살리면 DB 에는 발급이
     * 있는데 재고가 한 장 늘어난다 — <b>초과 발급 방향</b>이다. 그래서 S8 이 게이트가 닫힌
     * 상태에서 DB 를 조회해 그 답을 {@code ARGV[2]} 로 넘긴다.
     *
     * <p><b>멀쩡한 값은 건드리지 않는다</b>({@code -1}). 파손 판정은 세 스크립트와 같은
     * 패턴 하나로만 한다 — 여기만 기준이 느슨하면 살아 있는 선점을 지우게 되고,
     * 그것은 초과 발급 방향이다.
     */
    public static final RedisScript<Long> RECLAIM_CORRUPT = new DefaultRedisScript<>("""
            local function isCanonicalInt(s, signed)
                if s == false or type(s) == 'table' or #s > 15 then return false end
                if s == '0' then return true end
                if signed then return string.match(s, '^-?[1-9]%d*$') ~= nil end
                return string.match(s, '^[1-9]%d*$') ~= nil
            end
            local function readableCounters(stockKey, everKey)
                if not isCanonicalInt(redis.pcall('GET', stockKey), true) then return false end
                local rawEver = redis.pcall('GET', everKey)
                if rawEver == false then return true end               -- 키 부재는 예열이다
                return isCanonicalInt(rawEver, true)
            end
            if #ARGV < 3 then return -10 end
            if #ARGV[1] == 0 or (ARGV[2] ~= '0' and ARGV[2] ~= '1') then return -10 end
            if not isCanonicalInt(ARGV[3], false) then return -10 end
            local stored = redis.call('HGET', KEYS[2], ARGV[1])
            if stored == false then return 0 end
            local st, ms, tk, key = string.match(stored, '^([PD])|(%d+)|([^|]+)|(.+)$')
            if st ~= nil and #ms <= 13 then return -1 end
            if ARGV[2] == '0' then                                     -- DB 에 발급이 있다
                redis.call('HDEL', KEYS[2], ARGV[1])
                return 2
            end
            if not readableCounters(KEYS[1], KEYS[3]) then return -11 end
            local left = tonumber(redis.call('GET', KEYS[1]))
            if left + 1 > tonumber(ARGV[3]) then return -2 end         -- 짝 없는 INCR 은 초과 발급
            redis.call('HDEL', KEYS[2], ARGV[1])
            redis.call('INCR', KEYS[1])
            redis.call('DECR', KEYS[3])
            return 1
            """, Long.class);

    private IssuanceScripts() {
    }
}
