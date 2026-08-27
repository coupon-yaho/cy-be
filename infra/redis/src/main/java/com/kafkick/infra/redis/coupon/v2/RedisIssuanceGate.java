package com.kafkick.infra.redis.coupon.v2;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.coupon.v2.port.ClaimCommand;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;
import com.kafkick.core.coupon.v2.port.CompleteOutcome;
import com.kafkick.core.coupon.v2.port.GateMeta;
import com.kafkick.core.coupon.v2.port.GateStatus;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.ReclaimOutcome;
import com.kafkick.core.coupon.v2.port.RestoreOutcome;

/**
 * v2 게이트의 Redis 어댑터. <b>숫자 반환 코드는 이 클래스 안에서 끝난다.</b>
 *
 * <p>같은 {@code -2} 가 스크립트마다 다른 뜻이고(완료는 "남의 선점", 복원은 "상한 초과"),
 * {@code -1} 도 마찬가지다. 밖으로 흘리면 응답 매핑(S5)과 경보가 조용히 어긋난다.
 * 그래서 코드→결과 타입 변환표를 여기 한 곳에 두고, <b>표에 없는 코드는 예외</b>다 —
 * 스크립트가 새 코드를 내기 시작했다는 사실을 기본값으로 덮으면 그 순간부터 아무도 모른다.
 *
 * <p>{@code meta} 의 field 이름도 여기가 소유한다. 스크립트의 {@code HMGET} 이 다섯 이름을
 * 박아 두고 있으므로, 쓰는 쪽이 이름 하나를 달리 쓰면 게이트는 영구히 미준비다.
 */
public class RedisIssuanceGate implements IssuanceGatePort {

    static final String META_STATUS = "status";
    static final String META_OPEN_AT = "openAt";
    static final String META_CLOSE_AT = "closeAt";
    static final String META_GRADE_MASK = "gradeMask";
    static final String META_TOTAL_QUANTITY = "totalQuantity";

    private static final List<String> META_FIELDS = List.of(
            META_STATUS, META_OPEN_AT, META_CLOSE_AT, META_GRADE_MASK, META_TOTAL_QUANTITY);

    private static final Map<Long, ClaimOutcome> CLAIM_OUTCOMES = Map.ofEntries(
            Map.entry(IssuanceScriptCodes.Claim.OK, ClaimOutcome.CLAIMED),
            Map.entry(IssuanceScriptCodes.Claim.CLOSED, ClaimOutcome.CLOSED),
            Map.entry(IssuanceScriptCodes.Claim.NOT_OPEN, ClaimOutcome.NOT_OPEN),
            Map.entry(IssuanceScriptCodes.Claim.GRADE_NOT_ALLOWED, ClaimOutcome.GRADE_NOT_ALLOWED),
            Map.entry(IssuanceScriptCodes.Claim.DUP_PER_MEMBER, ClaimOutcome.DUP_PER_MEMBER),
            Map.entry(IssuanceScriptCodes.Claim.SOLD_OUT, ClaimOutcome.SOLD_OUT),
            Map.entry(IssuanceScriptCodes.Claim.REPLAY_DONE, ClaimOutcome.REPLAY_DONE),
            Map.entry(IssuanceScriptCodes.Claim.REPLAY_PENDING, ClaimOutcome.REPLAY_PENDING),
            Map.entry(IssuanceScriptCodes.Claim.CORRUPT_VALUE, ClaimOutcome.CORRUPT_VALUE),
            Map.entry(IssuanceScriptCodes.Claim.NOT_READY, ClaimOutcome.GATE_NOT_READY),
            Map.entry(IssuanceScriptCodes.Claim.BAD_ARGUMENT, ClaimOutcome.BAD_ARGUMENT),
            Map.entry(IssuanceScriptCodes.Claim.COUNTER_UNREADABLE, ClaimOutcome.COUNTER_UNREADABLE));

    private static final Map<Long, CompleteOutcome> COMPLETE_OUTCOMES = Map.of(
            IssuanceScriptCodes.Complete.PROMOTED, CompleteOutcome.PROMOTED,
            IssuanceScriptCodes.Complete.ALREADY_DONE, CompleteOutcome.ALREADY_DONE,
            IssuanceScriptCodes.Complete.CLAIM_GONE, CompleteOutcome.CLAIM_GONE,
            IssuanceScriptCodes.Complete.FOREIGN_CLAIM, CompleteOutcome.FOREIGN_CLAIM,
            IssuanceScriptCodes.Complete.CORRUPT_VALUE, CompleteOutcome.CORRUPT_VALUE,
            IssuanceScriptCodes.Complete.BAD_ARGUMENT, CompleteOutcome.BAD_ARGUMENT);

    private static final Map<Long, CompensateOutcome> COMPENSATE_OUTCOMES = Map.of(
            IssuanceScriptCodes.Compensate.REVERTED, CompensateOutcome.REVERTED,
            IssuanceScriptCodes.Compensate.NOT_MINE, CompensateOutcome.NOT_MINE,
            IssuanceScriptCodes.Compensate.ALREADY_DONE, CompensateOutcome.ALREADY_DONE,
            IssuanceScriptCodes.Compensate.CORRUPT_VALUE, CompensateOutcome.CORRUPT_VALUE,
            IssuanceScriptCodes.Compensate.COUNTER_UNREADABLE, CompensateOutcome.COUNTER_UNREADABLE,
            IssuanceScriptCodes.Compensate.BAD_ARGUMENT, CompensateOutcome.BAD_ARGUMENT);

    private static final Map<Long, RestoreOutcome> RESTORE_OUTCOMES = Map.of(
            IssuanceScriptCodes.Restore.RESTORED, RestoreOutcome.RESTORED,
            IssuanceScriptCodes.Restore.NOT_READY, RestoreOutcome.GATE_NOT_READY,
            IssuanceScriptCodes.Restore.OVER_CAP, RestoreOutcome.OVER_CAP,
            IssuanceScriptCodes.Restore.BAD_ARGUMENT, RestoreOutcome.BAD_ARGUMENT,
            IssuanceScriptCodes.Restore.STOCK_MISSING, RestoreOutcome.STOCK_MISSING);

    private static final Map<Long, ReclaimOutcome> RECLAIM_OUTCOMES = Map.of(
            IssuanceScriptCodes.Reclaim.RECLAIMED_AND_RESTORED, ReclaimOutcome.RECLAIMED_AND_RESTORED,
            IssuanceScriptCodes.Reclaim.RECLAIMED_ONLY, ReclaimOutcome.RECLAIMED_ONLY,
            IssuanceScriptCodes.Reclaim.NOTHING, ReclaimOutcome.NOTHING,
            IssuanceScriptCodes.Reclaim.NOT_CORRUPT, ReclaimOutcome.NOT_CORRUPT,
            IssuanceScriptCodes.Reclaim.COUNTER_UNREADABLE, ReclaimOutcome.COUNTER_UNREADABLE,
            IssuanceScriptCodes.Reclaim.OVER_CAP, ReclaimOutcome.OVER_CAP,
            IssuanceScriptCodes.Reclaim.BAD_ARGUMENT, ReclaimOutcome.BAD_ARGUMENT);

    /** 회수의 복원 여부 플래그. 스크립트가 {@code '0'}/{@code '1'} 만 받는다. */
    private static final String RESTORE_STOCK = "1";
    private static final String KEEP_STOCK = "0";

    private final IssuanceScriptRunner scriptRunner;
    private final StringRedisTemplate redisTemplate;

    public RedisIssuanceGate(IssuanceScriptRunner scriptRunner, StringRedisTemplate redisTemplate) {
        this.scriptRunner = scriptRunner;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public ClaimResult claim(ClaimCommand command) {
        List<?> returned = scriptRunner.claim(
                command.couponRoundId(),
                Long.toString(command.memberId()),
                Integer.toString(command.gradeBit()),
                command.idempotencyKey(),
                command.requestToken());
        if (returned == null || returned.isEmpty()) {
            throw new IllegalStateException("선점 스크립트가 아무 코드도 돌려주지 않았습니다.");
        }
        ClaimOutcome outcome = map(CLAIM_OUTCOMES, code(returned.getFirst(), "선점"), "선점");
        if (outcome != ClaimOutcome.CLAIMED) {
            return ClaimResult.rejected(outcome);
        }
        if (returned.size() < 2) {
            // 성공은 2원소다. 1원소면 스크립트와 어댑터가 갈린 것이라 0 으로 메우지 않는다 —
            // 0 은 "마지막 한 장" 으로 읽혀 그 뒤의 판단을 전부 오염시킨다.
            throw new IllegalStateException("선점 성공인데 잔여 재고가 없습니다.");
        }
        return ClaimResult.claimed(code(returned.get(1), "선점"));
    }

    @Override
    public CompleteOutcome complete(long couponRoundId, long memberId, String requestToken) {
        return map(COMPLETE_OUTCOMES,
                scriptRunner.complete(couponRoundId, Long.toString(memberId), requestToken), "완료");
    }

    @Override
    public CompensateOutcome compensate(long couponRoundId, long memberId, String requestToken) {
        return map(COMPENSATE_OUTCOMES,
                scriptRunner.compensate(couponRoundId, Long.toString(memberId), requestToken), "보상");
    }

    @Override
    public RestoreOutcome restore(long couponRoundId, long count) {
        return map(RESTORE_OUTCOMES,
                scriptRunner.restore(couponRoundId, Long.toString(count)), "복원");
    }

    @Override
    public ReclaimOutcome reclaimCorrupt(
            long couponRoundId, long memberId, boolean restoreStock, long totalQuantity) {
        return map(RECLAIM_OUTCOMES, scriptRunner.reclaimCorrupt(
                couponRoundId,
                Long.toString(memberId),
                restoreStock ? RESTORE_STOCK : KEEP_STOCK,
                Long.toString(totalQuantity)), "회수");
    }

    @Override
    public void writeMeta(long couponRoundId, GateMeta meta) {
        // 한 번에 쓴다. 다섯을 나눠 쓰면 그 사이에 도착한 선점이 부분 상태를 보고 -9 를 받는다.
        redisTemplate.opsForHash().putAll(IssuanceKeys.of(couponRoundId).meta(), Map.of(
                META_STATUS, meta.status().wireValue(),
                META_OPEN_AT, Long.toString(meta.openAtEpochMillis()),
                META_CLOSE_AT, Long.toString(meta.closeAtEpochMillis()),
                META_GRADE_MASK, Integer.toString(meta.gradeMask()),
                META_TOTAL_QUANTITY, Long.toString(meta.totalQuantity())));
    }

    @Override
    public Optional<GateMeta> readMeta(long couponRoundId) {
        List<Object> values = redisTemplate.opsForHash()
                .multiGet(IssuanceKeys.of(couponRoundId).meta(), List.copyOf(META_FIELDS));
        if (values == null || values.size() != META_FIELDS.size()) {
            return Optional.empty();
        }
        for (Object value : values) {
            if (value == null) {
                // 부분 상태다. 스크립트의 -9 와 같은 판정이라 여기서도 값을 만들지 않는다.
                return Optional.empty();
            }
        }
        try {
            return Optional.of(new GateMeta(
                    GateStatus.fromWireValue((String) values.get(0)),
                    Long.parseLong((String) values.get(1)),
                    Long.parseLong((String) values.get(2)),
                    Integer.parseInt((String) values.get(3)),
                    Long.parseLong((String) values.get(4))));
        } catch (IllegalArgumentException exception) {
            // 숫자가 아닌 meta 도 스크립트에는 -9 다. 읽는 쪽만 값을 지어내면 두 판정이 갈린다.
            return Optional.empty();
        }
    }

    private static <T> T map(Map<Long, T> table, long code, String script) {
        T outcome = table.get(code);
        if (outcome == null) {
            throw new IllegalStateException(
                    script + " 스크립트가 표에 없는 코드를 돌려줬습니다: " + code);
        }
        return outcome;
    }

    /**
     * 선점 반환 원소를 숫자로 읽는다. 스크립트의 결과 타입에는 원소 타입이 없어 캐스팅으로는
     * 아무것도 보장되지 않는다 — <b>읽는 자리에서</b> 확인하고, 아니면 그 자리에서 터뜨린다.
     * 여기서 삼키면 다음 실패는 잔여 재고를 쓰는 훨씬 뒤의 코드에서 나온다.
     */
    private static long code(Object value, String script) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalStateException(
                script + " 스크립트가 숫자가 아닌 값을 돌려줬습니다: "
                        + (value == null ? "null" : value.getClass().getSimpleName()));
    }
}
