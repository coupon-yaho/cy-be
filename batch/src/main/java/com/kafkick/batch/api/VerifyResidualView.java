// 두 검증 실행 사이에서 무엇이 남고 무엇이 새로 생겼는지입니다.
package com.kafkick.batch.api;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ResidualCount;
import com.kafkick.core.verification.VerificationRun;

/**
 * <b>{@code /reports/diff} 는 유형별 개수만 맞댄다 — 그것으로는 못 가르는 것이 있다.</b>
 *
 * <pre>
 * STOCK_MISMATCH  3 → 3
 *   ① 그 3건이 그대로 남았다        → 아무도 안 고치고 있다
 *   ② 3건이 고쳐지고 새로 3건 생겼다 → 지금도 계속 깨지고 있다
 * </pre>
 *
 * <p>처방이 정반대인데 diff 의 응답에서는 <b>같은 모양</b>이다. 갈라 주는 것은 개수가 아니라
 * {@code (finding_type, target_key)} 단위의 집합 연산이고, 이 조회가 그것을 낸다.
 *
 * <p>사전예약 PRD 대사 보고서 5단계가 요구하는 넷 중 <b>잔여 불일치</b> 축이다 —
 * 검사 건수는 CY-945 가 냈고, 정정·복구는 대사가 <b>쓰기</b>를 하는 축이라 예약 도메인이 진다.
 *
 * <h2>삭제가 이 대조를 안 끊는다 — 실측</h2>
 *
 * <p>{@code CleanupJdbcAdapter.deleteFindings} 가 검출 행을 지우므로 <i>"앞 실행에 없었다"</i>
 * 를 <b>행의 부재로 읽으면 안 되는 것 아닌가</b> 를 먼저 봤다. <b>안 끊는다</b> — 아래는
 * 프로브가 아니라 <b>세 자리를 읽어 확인한 연쇄</b>다.
 *
 * <ul>
 *   <li>지우는 대상이 {@code verdict IS NULL}(버려진 실행) 또는
 *       {@code dataset='CLEAN' AND verdict='PASS'} 둘뿐이다</li>
 *   <li>앞엣것은 이 조회가 애초에 안 받는다 — {@code closedRun} 이 거절한다</li>
 *   <li>뒤엣것은 <b>검출이 0건일 때만 PASS 다</b>({@code VerifyJobConfig.verdictOfClean} 은
 *       {@code detected == 0 ? PASS : FAIL}) — 지울 행이 없으므로 무행이다</li>
 * </ul>
 *
 * <p>즉 <b>닫힌 실행 둘 사이에서는 정리가 지운 것이 하나도 없다.</b> 그래서 가드를 안 둔다.
 *
 * @param schema 규칙 스키마. 두 실행이 다른 규칙으로 판정됐는지 사람이 보라고 싣는다
 * @param before 앞 실행
 * @param after 뒤 실행
 * @param byType 규칙별 갈래. <b>검출이 없는 규칙도 0 으로 낸다</b>
 * @param remaining 뒤 실행에 남아 있는 총 건수. <b>PRD 가 말하는 "잔여 불일치 건수"</b>
 * @param introduced 그 사이에 새로 생긴 총 건수
 * @param resolved 그 사이에 사라진 총 건수
 */
public record VerifyResidualView(
        String schema,
        VerifyReportDiffView.Side before,
        VerifyReportDiffView.Side after,
        List<RuleResidual> byType,
        int remaining,
        int introduced,
        int resolved) {

    /**
     * @param persisted 두 실행에 다 있다. <b>이 수가 안 줄면 아무도 안 고치고 있는 것</b>이다
     * @param introduced 뒤 실행에만 있다
     * @param resolved 앞 실행에만 있다
     */
    public record RuleResidual(FindingType type, int persisted, int introduced, int resolved) {

        /**
         * <b>정의를 여기서 다시 쓰지 않는다.</b> <i>"잔여 = 지속 + 신규"</i> 를 두 곳에
         * 적으면 한쪽만 고치는 날 규칙별 합과 총합이 갈린다 —
         * {@link ResidualCount#remaining()} 이 그 정의의 자리다.
         */
        public int remaining() {
            return new ResidualCount(persisted, introduced, resolved).remaining();
        }
    }

    /**
     * <b>검출이 없는 규칙도 0 으로 낸다.</b> 빼면 <i>"그 규칙을 봤는데 없었다"</i> 와
     * <i>"그 규칙이 아예 안 돌았다"</i> 가 응답에서 같은 모양이 된다 —
     * {@code /runs/stuck} 이 빈 잡을 내는 것과 같은 근거다.
     *
     * <p>순서는 {@code FindingType.values()} 다. 맵의 순회 순서에 기대면 제출물이
     * 실행마다 달라진다 — {@code VerifyReportView.of} 가 같은 이유로 같은 모양을 쓴다.
     */
    public static VerifyResidualView of(String schema,
            VerificationRun was, int beforeCount,
            VerificationRun now, int afterCount,
            Map<FindingType, ResidualCount> residual) {

        List<RuleResidual> byType = Arrays.stream(FindingType.values())
                .map(type -> {
                    ResidualCount count = residual.getOrDefault(type, ResidualCount.NONE);
                    return new RuleResidual(type, count.persisted(),
                            count.introduced(), count.resolved());
                })
                .toList();

        return new VerifyResidualView(schema,
                VerifyReportDiffView.Side.of(was, beforeCount),
                VerifyReportDiffView.Side.of(now, afterCount),
                byType,
                byType.stream().mapToInt(RuleResidual::remaining).sum(),
                byType.stream().mapToInt(RuleResidual::introduced).sum(),
                byType.stream().mapToInt(RuleResidual::resolved).sum());
    }
}
