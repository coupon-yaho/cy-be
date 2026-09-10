// 전후 비교에서 어느 대상이 남고 어느 것이 새로 생겼는지의 한 페이지입니다.
package com.kafkick.batch.api;

import java.util.List;

import com.kafkick.core.verification.ResidualCursor;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.ResidualKind;
import com.kafkick.core.verification.ResidualTarget;

/**
 * <b>{@link VerifyResidualView} 가 접어 버린 그레인을 편다.</b>
 *
 * <p>집계는 <i>"STOCK_MISMATCH 가 3건 남았다"</i> 까지 답하고 멈춘다. 그다음 질문
 * (<i>"어느 회차인가"</i>)은 이 응답이 답한다. 그 링크가 없으면 보고서 항목에서
 * 실제 대상으로 갈 방법이 없다.
 *
 * <p><b>일부러 다른 타입이다.</b> 집계에 목록을 끼워 넣으면 <i>"몇 건 남았나"</i> 만
 * 알고 싶은 화면도 대상 전량을 받는다 — 12만 키 형상에서 <b>10.7MB</b> 다.
 *
 * <h2>{@code nextCursor} 를 읽는 법</h2>
 *
 * <p><b>{@code null} 이면 마지막 페이지다.</b> 돌려준 줄이 {@code limit} 보다 적을 때만
 * {@code null} 이 된다 — 정확히 {@code limit} 만큼 왔다면 <b>다음 페이지가 비어 있을
 * 수도 있다.</b> 그 한 번의 헛걸음을 없애려면 한 줄을 더 읽어 봐야 하는데, 이 질의는
 * 페이지마다 집합을 다시 만들어서 <b>그 확인이 페이지 하나 값이다.</b> 헛걸음이 싸다.
 *
 * @param schema 검사한 스키마
 * @param before 앞 실행
 * @param after 뒤 실행
 * @param kind 좁힌 종류. {@code null} 이면 전부
 * @param targets 이 페이지의 대상들. 정렬 순서는 {@code (유형, 대상키)} 다
 * @param nextCursor 다음 요청의 {@code cursor} 파라미터에 <b>그대로</b> 실어 보낸다.
 *        {@code null} 이면 끝. Base64URL 한 덩어리라 인코딩할 것이 없다 — 왜 그 모양인지는
 *        {@link com.kafkick.core.verification.ResidualCursor#encode()} 에 적었다
 */
public record VerifyResidualTargetsView(
        String schema,
        VerifyReportDiffView.Side before,
        VerifyReportDiffView.Side after,
        ResidualKind kind,
        List<Target> targets,
        String nextCursor) {

    /**
     * 한 줄.
     *
     * <p><b>{@code expected}·{@code actual} 이 없다.</b> 자유 문자열이라 포맷 한 글자에
     * 같은 대상이 다르게 보인다 — {@code findings_checksum} 이 같은 이유로 뺐다.
     * 상세가 필요하면 {@code targetKey} 로 그 실행의 검출을 조회한다.
     */
    public record Target(String type, String targetKey, ResidualKind kind) {
    }

    public static VerifyResidualTargetsView of(String schema,
            VerificationRun was, int beforeCount,
            VerificationRun now, int afterCount,
            ResidualKind kind, List<ResidualTarget> page, int limit) {

        List<Target> targets = page.stream()
                .map(t -> new Target(t.type().name(), t.targetKey(), t.kind()))
                .toList();

        String next = page.size() < limit ? null
                : ResidualCursor.after(page.get(page.size() - 1)).encode();

        return new VerifyResidualTargetsView(schema,
                VerifyReportDiffView.Side.of(was, beforeCount),
                VerifyReportDiffView.Side.of(now, afterCount),
                kind, targets, next);
    }
}
