// 두 검증 실행의 검출을 규칙별로 맞댄 결과입니다.
package com.kafkick.batch.api;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;

/**
 * <b>"고쳐졌다" 를 보이려면 두 실행이 나란히 있어야 한다.</b>
 *
 * <p>한 실행의 판정만으로는 <i>"원래 0건이었다"</i> 와 <i>"고쳐서 0건이 됐다"</i> 가
 * 구분되지 않는다. 정합률 100%를 <b>주장</b>이 아니라 <b>증거</b>로 만드는 것이 이 뷰다.
 *
 * <p><b>규칙별로 낸다.</b> 총합만 주면 <i>"한 규칙이 줄고 다른 규칙이 늘었는데 합이
 * 같은"</i> 경우가 <b>변화 없음</b>으로 보인다 — 그것이 제일 위험한 상태다.
 *
 * <p><b>{@code schema} 를 함께 낸다.</b> {@code dataset} 만으로는 화면이 카드를 못 가른다 —
 * 정상셋 배치와 운영 배치가 둘 다 {@code CLEAN} 이라 <b>이름표가 같은 카드 두 장</b>이
 * 된다. {@code VerifyReportView} 가 같은 이유로 싣고, 그 자리에 <i>"cy-fe 가 겪었다"</i>
 * 고 적혀 있다.
 *
 * <p><b>검출이 0인 규칙도 채운다.</b> {@code /reports/latest} 가 같은 이유로 그렇게 한다 —
 * {@code GROUP BY} 는 없는 것을 못 만들어서, 안 채우면 <i>"그 규칙이 0건"</i> 과
 * <i>"그 규칙을 안 돌렸다"</i> 가 응답에서 같아진다.
 */
public record VerifyReportDiffView(
        String schema,
        Side before,
        Side after,
        List<RuleDelta> byType,
        int totalDelta,
        boolean verdictChanged) {

    /**
     * 맞댄 한쪽.
     *
     * <p><b>번호만 실으면 응답이 자기를 설명하지 못한다.</b> 저장된 JSON 하나를 놓고
     * <i>"{@code before=17} 이 무엇이었나"</i> 를 DB 로 다시 물어야 한다 — 형제
     * {@code VerifyProgressView}·{@code VerifyHistoryView} 는 전부 이 축을 싣는다.
     *
     * <p><b>{@code before} 와 {@code after} 를 뒤집어 넣은 것도 여기서 드러난다.</b>
     * 시각이 없으면 화면이 <i>"고쳤더니 검출이 800건 늘었다"</i> 를 조용히 그린다 —
     * 거절할 일은 아니지만(어느 둘을 맞댈지는 부르는 쪽이 고른다) 사람이 알아챌 수는
     * 있어야 한다.
     */
    public record Side(
            long runId,
            DatasetType dataset,
            ScopeType scope,
            int attempt,
            LocalDateTime asOf,
            LocalDateTime startedAt,
            VerdictType verdict,
            int findingCount) {

        static Side of(VerificationRun run, int findingCount) {
            return new Side(run.id(), run.dataset(), run.scope(), run.attempt(),
                    run.asOf(), run.startedAt(), run.verdict(), findingCount);
        }
    }

    /**
     * 규칙 하나의 변화. {@code delta} 는 <b>{@code after - before}</b> 라 <b>음수가
     * 줄어든 것</b>이다 — 대사가 성공하면 음수가 나온다.
     */
    public record RuleDelta(FindingType type, int before, int after, int delta) {
    }

    /**
     * @param before 고치기 전 실행. 시각이 앞선 쪽이 아니라 <b>부르는 쪽이 지정한</b> 쪽이다
     * @param after  고친 뒤 실행
     */
    public static VerifyReportDiffView of(String schema,
            VerificationRun before, Map<FindingType, Integer> b,
            VerificationRun after, Map<FindingType, Integer> a) {

        List<RuleDelta> deltas = Stream.of(FindingType.values())
                .map(type -> {
                    int was = b.getOrDefault(type, 0);
                    int now = a.getOrDefault(type, 0);
                    return new RuleDelta(type, was, now, now - was);
                })
                .toList();

        int total = deltas.stream().mapToInt(RuleDelta::delta).sum();

        return new VerifyReportDiffView(
                schema,
                Side.of(before, sum(b)),
                Side.of(after, sum(a)),
                deltas,
                total,
                before.verdict() != after.verdict());
    }

    private static int sum(Map<FindingType, Integer> counts) {
        return counts.values().stream().mapToInt(Integer::intValue).sum();
    }
}
