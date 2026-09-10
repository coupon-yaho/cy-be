// 검출 결과 저장 계약입니다. 규칙 6종이 전부 이 계층으로 결과를 남깁니다.
package com.kafkick.core.verification;

import java.util.List;
import java.util.Map;

/**
 * 읽기는 여기에 없습니다. 판정(양방향 MINUS·checksum)은 300만 행을 자바로 끌어올리지 않고
 * 집계 SQL 로 하므로 판정 쪽이 직접 질의합니다.
 */
public interface VerificationFindingRepository {

    /**
     * 대상 목록 한 페이지의 상한.
     *
     * <p><b>포트에 있는 이유</b> — 어댑터와 API 가 <b>같은 값</b>을 강제해야 한다.
     * 두 벌로 두면 한쪽만 고쳐질 때 API 는 받아 놓고 어댑터가 던진다(그러면 부르는 쪽이
     * 고칠 수 있는 잘못이 <b>500</b> 으로 나간다).
     *
     * <p><b>시간이 아니라 바이트에서 나온 값이다.</b> 실측으로 페이지 크기는 시간을
     * 거의 안 바꾼다(12만 키 형상에서 100건 220ms · 10,000건 240ms) — 집합을 다 만들고
     * 자르기 때문이다. 바뀌는 것은 응답 크기다:
     *
     * <pre>
     *     200행   짧은 키 14KB · 긴 키 18KB     ← 기본값
     *   1,000행   짧은 키 70KB · 긴 키 89KB
     *  10,000행   0.7 ~ 0.9MB
     * 120,000행   8.4 ~ 10.7MB                  ← 상한 없는 전량
     * </pre>
     *
     * <p>행당 <b>70B(짧은 키) ~ 89B(긴 키)</b> 다. 긴 쪽은 {@code DUP_PER_MEMBER} 의
     * {@code COUPON:{n}|MEMBER:{m}} 이다. 120,000 은 <b>규칙당 상한 10,000 × 6종 ×
     * 두 실행에서 겹침이 0일 때</b>이고, 형제 {@code ResidualQueryCostProbe} 가 같은 수를
     * {@code CAP_KEYS} 로 못 박아 뒀다.
     *
     * <p>⚠️ <b>한때 여기 31.5B/행(1,000행 30KB · 전량 2.7MB)이 적혀 있었다.</b> 프로브가
     * <b>응답이 아니라 DB 행 텍스트</b>를 셌던 것이다 — 잰 값이 "안 쟀다" 보다 나빴다.
     * 지금 값은 실제 응답 객체를 같은 매퍼로 직렬화해 센 것이다.
     *
     * <p>1,000 은 한 응답을 <b>100KB 아래</b>에 묶는 자리다. 더 키우면 시간은 그대로인데
     * 응답만 커지고, 더 줄이면 <b>페이지 수가 늘어 총 시간이 는다</b> — 페이지마다
     * 집합을 다시 만들기 때문이다.
     */
    int MAX_TARGET_PAGE = 1_000;

    /**
     * 검출 결과를 한 묶음으로 쌓는다.
     *
     * <p>같은 행을 다시 써도 되게 만든다. {@code uk_run_finding(run_id, finding_type, target_key)}
     * 가 걸려 있어 그냥 INSERT 면 같은 검출이 두 번 나오는 순간 중복키로 죽는다.
     * 지금 배선에서 재시작은 막혀 있지만(`preventRestart`), 청크 롤백 후 재실행과
     * 앞으로의 변경까지 덮으려면 멱등이 맞다.
     *
     * <p>행 수를 돌려주지 않는다. 배치 재작성이 켜지면 드라이버가 행마다
     * {@code SUCCESS_NO_INFO} 를 준다. 검출 건수는 판정 단계가 집계 SQL 로 따로 센다.
     */
    void appendAll(long runId, List<VerificationFinding> findings);

    /**
     * 이 실행의 검출 수. 판정과 함께 {@code verification_runs} 에 남습니다.
     *
     * <p>규칙 Step 이 센 것을 더하지 않고 <b>저장된 행을 다시 셉니다.</b> 더하면
     * {@code uk_run_finding} 이 접은 중복이 개수에만 남아, checksum 과 어긋난 수가 기록됩니다.
     */
    int countOf(long runId);

    /**
     * <b>두 실행의 검출을 {@code (finding_type, target_key)} 단위로 맞댄다.</b>
     * 유형별 개수만 맞대는 {@code /reports/diff}(CY-944)가 <i>"같은 3건"</i> 과
     * <i>"다 고쳐지고 새로 3건"</i> 을 못 가르는 자리를 메운다.
     *
     * <p><b>집계는 DB 가 접는다.</b> 키 집합을 자바로 올리면 규칙당 상한
     * ({@code batch.verify.max-findings-per-rule}, 기본 10,000)에 규칙 여섯을 곱해
     * 한 실행이 최대 6만 키다 — 두 실행이면 12만이고, 이 조회는 관리자 API 의
     * 5초 예산 안에서 돌아야 한다. 그래서 돌려주는 것은 <b>규칙 수만큼의 행</b>이다.
     *
     * <p><b>한 키가 한 실행에 두 번 못 나오는 것이 이 집계의 전제다</b> —
     * {@code uk_run_finding} 이 그것을 막는다. 그 유니크가 사라지면 "두 실행에 다 있다"
     * 판정이 같은 실행 안의 중복으로도 성립해 <b>지속을 과대 보고</b>한다.
     *
     * @param beforeRunId 앞 실행. 닫힌 실행이어야 한다 — 부르는 쪽이 지킨다
     * @param afterRunId 뒤 실행
     * @return 검출이 하나라도 있는 규칙만. 없는 규칙은 키가 없다
     */
    Map<FindingType, ResidualCount> residualByType(long beforeRunId, long afterRunId);

    /**
     * 전후 비교를 <b>대상 단위</b>로 한 페이지 읽는다.
     *
     * <p>{@link #residualByType} 이 <i>"이 유형이 몇 건"</i> 을 답하고 이쪽이
     * <i>"어느 대상"</i> 을 답한다. 둘 중 하나만 있으면 화면이 <b>"3건 남았다" 까지만
     * 알고 무엇을 조치해야 하는지 모른다.</b>
     *
     * <p><b>집계를 이것으로 대신하지 말 것.</b> 접힌 집계는 12만 키에서 6행을 돌려주고,
     * 이 목록은 같은 형상에서 <b>최대 12만 행</b>이다. 첫 질문("몇 건 남았나")에는 그쪽이 맞다.
     *
     * @param kind 이 종류만. {@code null} 이면 전부.
     *        <b>시간을 줄이지 않는다</b> — 집합은 어차피 다 만들어진다(실측: 좁혀도
     *        193ms). 줄어드는 것은 바이트와 페이지 수다
     * @param cursor 이 자리 뒤부터. {@code null} 이면 처음부터
     * @param limit 한 페이지 최대 건수
     * @return 정렬된 한 페이지. {@code limit} 보다 적으면 마지막 페이지다
     */
    List<ResidualTarget> residualTargets(long beforeRunId, long afterRunId,
            ResidualKind kind, ResidualCursor cursor, int limit);

    /**
     * 검출 집합의 checksum. <b>재실행 결정론 판정의 근거</b>입니다.
     *
     * <p>계약({@code docs/contract.json} 의 {@code findings_checksum})이 정한 인코딩을 그대로 씁니다 —
     * 정렬된 {@code (finding_type, target_key)} 만, {@code finding_type + U+001F + target_key + U+001E}
     * 를 반복한 뒤 SHA-256. <b>다른 컬럼을 섞으면 안 됩니다</b> — {@code id} 나 시각이 들어가면
     * 같은 데이터의 재실행이 매번 다른 값을 냅니다.
     *
     * <p><b>DB 에서 접지 않고 자바에서 흘려 계산합니다.</b> {@code GROUP_CONCAT} 은
     * {@code group_concat_max_len} 을 넘으면 경고만 내고 조용히 잘라, 오염셋 800행에서
     * <b>뒤쪽 검출이 checksum 에 안 들어갑니다</b> — 결정론 판정이 열린 채로 통과합니다.
     *
     * <p>검출이 없으면 빈 입력의 SHA-256 입니다. {@code null} 이 아닙니다 —
     * 정상셋의 0건은 <b>판정 대상</b>이지 미실행이 아닙니다.
     */
    String checksumOf(long runId);

    /**
     * 이 실행의 검출을 <b>규칙별로</b> 센다. 제출용 리포트가 읽는다.
     *
     * <p><b>검출이 0인 규칙은 안 나온다.</b> {@code GROUP BY} 가 없는 것을 못 만들기 때문이다 —
     * 리포트가 여섯 규칙을 다 보여 주려면 {@link FindingType} 을 기준으로 채워야 한다.
     * 그 자리를 여기서 메우지 않는 것은, <b>규칙 목록의 주인이 이 포트가 아니기</b> 때문이다.
     *
     * <p>합계는 {@link #countOf} 와 같아야 한다. 다르면 {@code finding_type} 에 전이표 밖의
     * 값이 들어간 것이고, 그때는 리포트가 아니라 규칙 쪽을 봐야 한다.
     *
     * @return 검출이 있는 규칙만. 순서는 {@code finding_type} 오름차순으로 고정한다 —
     *         다만 리포트는 이 순서를 안 쓴다({@code VerifyReportView.of} 가 다시 채운다)
     */
    Map<FindingType, Integer> countByType(long runId);
}
