package com.kafkick.batch.api;

import java.time.LocalDateTime;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;

/**
 * 검증 실행 이력 한 줄.
 *
 * <p>core 레코드를 그대로 내보내지 않는다. 그러면 HTTP 계약이 도메인 모델에 묶여, 거기에
 * 필드를 더하는 다음 티켓이 응답 스키마를 의도 없이 바꾼다 — 그 티켓의 diff 에는 이
 * 컨트롤러가 없어 리뷰에서도 안 잡힌다. verification_runs 에는 이미 레코드가 아직 안 받은
 * origin 컬럼이 있다.
 *
 * <p>⚠️ <b>증분이 열리기 전까지 fromTs 는 언제나 null 이다.</b> {@code rejectUnsupportedScope}
 * 가 INCREMENTAL 을 막고 있고({@code docs/15} "남긴 것"), FULL 은 {@code fromTs} 가 null
 * 이어야 한다({@code VerificationRun} 생성자가 강제한다). 그래도 지금 넣는 이유는 아래다 —
 * 증분이 열리는 날 응답 스키마를 바꾸면 그때 화면도 같이 고쳐야 한다.
 *
 * <p>scope 를 싣는 이상 창의 시작도 싣는다. INCREMENTAL 은 (fromTs, asOf] 가 곧 입력이라,
 * fromTs 없이는 같은 asOf·같은 scope 인 두 실행이 응답에서 완전히 같아 보이면서
 * findingsChecksum 만 다르다 — 그러면 체크섬 차이가 결정론 위반인지 창이 다른 건지 못 가른다.
 *
 * <p>seedRunId 는 안 싣는다. 그것은 CORRUPT 정답셋 식별자이고 VerifyReportView 몫이다.
 *
 * <p>findingCount 는 판정 전에는 비운다. VerificationRun.start 가 0 으로 시작하는데,
 * 이 프로젝트에서 0 은 "정합성 합격" 의 신호값이다 — 도는 중인 실행이 무결로 읽힌다.
 * findingsChecksum 도 같은 축이라 함께 비어 있다.
 *
 * <p><b>그 불변식은 쓰기 쪽이 이미 지킨다</b> — INSERT 가 findings_checksum 을 안 넣고
 * (그래서 NULL), 그것을 채우는 유일한 UPDATE 가 verdict 를 <b>같은 문장에서</b> 함께 쓴다
 * (VerificationRunJdbcAdapter). 그래도 여기서 한 번 더 가리는 것은, 이 주석이 약속한 것을
 * <b>이 파일의 코드가 스스로 지켜야</b> 읽는 사람이 쓰기 쪽까지 안 가도 되기 때문이다 —
 * findingCount 가 이미 같은 모양이라 둘이 어긋나 보이는 것도 이유다(봇 리뷰가 그것을 짚었다).
 */
public record VerifyHistoryView(
        Long runId,
        DatasetType dataset,
        ScopeType scope,
        int attempt,
        LocalDateTime asOf,
        LocalDateTime fromTs,
        VerdictType verdict,
        StatsStatus statsStatus,
        Integer findingCount,
        String findingsChecksum,
        String datasetFingerprint,
        LocalDateTime startedAt,
        LocalDateTime finishedAt) {

    public static VerifyHistoryView of(VerificationRun run) {
        return new VerifyHistoryView(
                run.id(),
                run.dataset(),
                run.scope(),
                run.attempt(),
                run.asOf(),
                run.fromTs(),
                run.verdict(),
                run.statsStatus(),
                run.verdict() == null ? null : run.findingCount(),
                run.verdict() == null ? null : run.findingsChecksum(),
                run.datasetFingerprint(),
                run.startedAt(),
                run.finishedAt());
    }
}
