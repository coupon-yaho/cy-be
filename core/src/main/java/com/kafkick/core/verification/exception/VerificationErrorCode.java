// 검증 배치의 도메인 에러입니다. core.support.ErrorCode 규약(<도메인>-<3자리>)을 따릅니다.
package com.kafkick.core.verification.exception;

import com.kafkick.core.support.exception.ErrorCode;

/**
 * 배치는 HTTP 서버가 아니지만 관리 포트로 verify 트리거 API 를 열기 때문에 status 를 채웁니다.
 * 배치 내부에서만 발생하는 오류는 500 을 씁니다.
 */
public enum VerificationErrorCode implements ErrorCode {

    INVALID_AS_OF(
            400,
            "VERIFICATION-001",
            "검증 기준 시각이 올바르지 않습니다."
    ),
    INVALID_RUN_PARAMS(
            400,
            "VERIFICATION-002",
            "검증 실행 파라미터가 올바르지 않습니다."
    ),

    /** 조회용 400대다. 배치 내부 경로에서는 {@link #RUN_ROW_VANISHED} 를 쓴다 — verify 트리거 API 몫이다. */
    RUN_NOT_FOUND(
            404,
            "VERIFICATION-003",
            "검증 실행을 찾을 수 없습니다."
    ),
    ASOF_STATE_MISSING(
            500,
            "VERIFICATION-004",
            "asOf 시점 상태가 준비되지 않았습니다."
    ),
    ILLEGAL_REPLAY_STATE(
            500,
            "VERIFICATION-005",
            "이력 리플레이 중 처리할 수 없는 상태를 만났습니다."
    ),
    DATASET_MUTATED_DURING_RUN(
            500,
            "VERIFICATION-006",
            "검증 실행 중에 대상 데이터가 바뀌어 이 실행의 검출을 신뢰할 수 없습니다."
    ),
    MANIFEST_ABSENT(
            500,
            "VERIFICATION-007",
            "오염셋 정답 매니페스트가 없습니다. 시드 주입을 먼저 실행하십시오."
    ),

    /**
     * 실행 중에 {@code verification_runs} 행이 사라졌다. 클라이언트가 뭘 잘못 던진 것이 아니라
     * 데이터 정합 사고라 500 이다 — 404 로 두면 재고 소진 같은 정상 흐름 예외와 같은 취급을 받는다.
     */
    RUN_ROW_VANISHED(
            500,
            "VERIFICATION-008",
            "검증 실행 행이 실행 도중에 사라졌습니다."
    ),

    /**
     * 만료 배치가 도는 중이다. 던진 파라미터가 아니라 <b>지금 런타임의 상태</b>가 문제라
     * 400 이 아니다 — 400 이면 자동화가 "파라미터를 고쳐 재시도" 루프에 빠져 같은 자리에서
     * 무한히 죽는다.
     *
     * <p>예전에는 {@code batch.scheduling.enabled} 가 켜져 있기만 해도 났다. 그것은
     * <i>"만료 스케줄러 빈이 만들어졌는가"</i>라 운영에서는 늘 참이었고, 그래서
     * <b>검증이 영영 못 돌았다.</b> 지금은 실행 중인 만료가 실제로 있을 때만 난다.
     *
     * <p><b>그 변화에도 500 을 유지한다.</b> 조건이 <i>"몇 초 뒤 사라지는 일시 상태"</i>로
     * 바뀌었으니 409 가 맞아 보이지만, <b>이 코드는 HTTP 로 나가지 않는다</b> — 잡이 비동기라
     * 실패는 {@code GET /runs/{id}} 의 문구로 실릴 뿐이다. 여기서 status 가 뜻하는 것은
     * <b>"판정을 못 냈다"</b> 는 축이고, 접수 단계의 같은 조건은
     * {@link #VERIFY_EXPIRE_RUNNING}(409)이 따로 진다. 그 차이는 의도다.
     */
    RUNTIME_NOT_QUIESCED(
            500,
            "VERIFICATION-009",
            "만료 배치가 실행 중이라 검증을 시작할 수 없습니다."
    ),

    /**
     * 발급건의 {@code ISSUE} 이력이 정확히 하나가 아니다(없거나 둘 이상).
     * 데이터 정합 사고이므로 500 이다.
     *
     * <p><b>{@code DATASET_MUTATED_DURING_RUN} 과 갈라야 한다.</b> 그쪽은 <b>재시도로 낫는다</b> —
     * 쓰기를 멈추고 다시 돌리면 통과한다. 이쪽은 멈춰도 같은 자리에서 영원히 죽는다.
     * 원인이 데이터 변동이 아니라 구조 파손이라, 데이터를 고치는 것 말고는 길이 없다.
     *
     * <p><b>메시지 문구로 가르지 않는다.</b> 던지는 자리가 아홉이고 문구에 통일된 규칙이
     * 없다 — 어느 자리는 조치를 적고 어느 자리는 왜 이 실행을 믿을 수 없는지만 적는다.
     * 가르는 기준은 문구가 아니라 <b>재시도 가능성</b>이다.
     */
    ISSUE_HISTORY_NOT_EXACTLY_ONE(
            500,
            "VERIFICATION-010",
            "ISSUE 이력이 정확히 하나가 아닌 발급건이 있습니다."
    ),

    /**
     * 기동 시점 가드다. 잡 실행 중이 아니라 <b>컨텍스트가 뜨는 동안</b> 나므로 HTTP 로 나갈 일이
     * 없지만, 규약상 status 를 비워 둘 수 없어 500 을 쓴다.
     */
    SCHEMA_NOT_MIGRATED(
            500,
            "VERIFICATION-011",
            "배치가 보는 스키마에 핵심 테이블이 없습니다."
    ),

    /**
     * 트리거 API 가 접수 단계에서 먼저 답하는 것이다. 같은 판정을 {@code startRunStep} 의
     * {@code rejectRunningExpire} 가 다시 하므로, 이 검사를 지워도 잡은 여전히 거절한다.
     *
     * <p><b>{@link #RUNTIME_NOT_QUIESCED}(500) 와 같은 조건이다.</b> 상태 코드가 다른 것은
     * 의도한 것이다 — 저쪽은 잡 안에서 나고 <i>"파라미터를 고쳐 재시도"</i> 루프를 막으려고
     * 5xx 를 골랐다. 이쪽은 HTTP 접수 단계라 <b>409(지금 상태가 아니다)</b> 가 맞고, 그것은
     * 재시도 루프 논거와 충돌하지 않는다. 둘 중 하나를 고칠 때는 반드시 함께 본다.
     */
    VERIFY_EXPIRE_RUNNING(
            409,
            "VERIFICATION-012",
            "만료 배치가 도는 동안에는 검증을 시작할 수 없습니다."
    ),

    /**
     * 실행기가 스레드 하나 · 큐 없음이라 미루지 않고 거절한다. 큐에 넣으면 그 사이
     * {@code asOf} 가 지나가 <b>접수 시점과 실행 시점이 다른 데이터를 본다.</b>
     */
    VERIFY_ALREADY_RUNNING(
            429,
            "VERIFICATION-013",
            "검증이 이미 실행 중입니다."
    ),

    /** 조회·중단이 못 찾은 경우다. {@code runId} 가 아니라 {@code executionId} 로 찾는다. */
    VERIFY_EXECUTION_NOT_FOUND(
            404,
            "VERIFICATION-014",
            "검증 실행을 찾을 수 없습니다."
    ),

    /** 이미 끝난 실행은 멈출 수 없다. 사건이 아니라 상태다. */
    VERIFY_NOT_RUNNING(
            409,
            "VERIFICATION-015",
            "그 검증 실행은 이미 끝났습니다."
    ),

    /**
     * {@code abandon} 은 {@code STARTED} 를 거부한다 — 살아 있는 프로세스가 정말 돌고 있을
     * 수도 있어 함부로 못 버린다. 먼저 {@code stop} 으로 {@code STOPPING} 을 만들어야 한다.
     */
    VERIFY_NOT_ABANDONABLE(
            409,
            "VERIFICATION-016",
            "먼저 중단 신호를 보내야 그 실행을 버릴 수 있습니다."
    ),

    /**
     * <b>도는 검증은 못 멈춘다.</b> 진도가 멈춘 실행만 {@code stop} 을 받는다.
     *
     * <p><b>왜 막나.</b> {@code stop} 은 살아 있는 실행에도 먹는다 — Spring Batch 6.0.4 의
     * {@code SimpleJobOperator.stop} 이 {@code endTime} 을 채워 넘기고
     * {@code SimpleJobRepository.update} 가 그것을 보고 <b>즉시 {@code STOPPED} 로 올린다</b>
     * (바이트코드로 확인). 그 순간 셋이 함께 풀린다 — 트리거의 429,
     * {@code ExpireScheduler} 의 슬롯 건너뛰기, {@code CleanupJobConfig} 의 물러나기.
     * <b>스레드는 아직 도는데 만료·정리가 그 입력을 건드리기 시작한다.</b>
     *
     * <p><b>대가는 30분이다.</b> 하드킬 직후 {@code batch.stuck-job-after-ms}(기본 30분)가
     * 지나야 시체로 판정된다. 그 전에는 이 코드로 거절되고, 메시지가 남은 시간을 말해 준다 —
     * 안 말해 주면 사람이 API 가 깨진 줄 안다.
     */
    VERIFY_EXECUTION_NOT_STUCK(
            409,
            "VERIFICATION-019",
            "지금 멈출 수 있는 실행이 아닙니다. 진도가 멈춘 실행만 중단할 수 있습니다."
    ),

    /**
     * <b>곧 뜰 만료와 겹칠 접수를 막는다.</b> 위 {@link #VERIFY_EXPIRE_RUNNING} 은 <i>이미
     * 도는</i> 만료를 배치 메타에서 보고, 이쪽은 <i>곧 뜰</i> 만료를 크론에서 본다 —
     * 둘 다 있어야 창이 닫힌다.
     *
     * <p>이 코드가 생긴 것은 {@code max-expire-skips} 가 <b>0</b> 이 되면서다(CY-470).
     * 그전에는 만료가 검증을 한 번은 건너뛰어 줬지만, 이제는 <b>첫 충돌에서 그대로
     * 지나간다</b> — 그때 찍히는 {@code issuances.updated_at} 때문에 그 {@code asOf} 는
     * 영구히 못 쓴다(재시딩 말고 복구가 없다).
     *
     * <p><b>문구가 처방을 말한다.</b> 예외 핸들러는 {@code detail} 을 로그에만 남기고
     * 응답에는 이 고정 문구만 싣는다(요청값이 새지 않게). 그래서 <i>"지금은 안 된다"</i> 만
     * 적으면 운영자가 <b>같은 시각에 또 누른다</b> — 무엇을 피해야 하는지가 여기 있어야 한다.
     * 정확한 시각은 배치 로그의 {@code detail} 에 있다.
     */
    VERIFY_EXPIRE_ABOUT_TO_FIRE(
            409,
            "VERIFICATION-017",
            "만료 크론이 곧 뜹니다. 만료는 이 검증을 건너뛰지 않고 지나가며 그때 이 asOf 는 "
                    + "영구히 못 쓰게 됩니다 — 배치 창(만료·정리·검증)을 지난 뒤 다시 "
                    + "부르십시오. 정확한 시각은 배치 로그에 있습니다."
    ),

    /**
     * {@code verification_findings.finding_type} 에 {@link
     * com.kafkick.core.verification.FindingType} 에 없는 값이 들어 있다.
     *
     * <p><b>그 컬럼에 CHECK 제약이 없다</b>({@code varchar(40)} + 주석뿐). 규칙을 하나 더해
     * 행을 쓴 뒤 코드를 되돌리면 그 상태가 된다.
     *
     * <p><b>죽는 것은 맞지만 500 으로 죽으면 안 된다.</b> 이 조회는 D13 제출물을 뜨는
     * 자리라, 한 행 때문에 죽으면 <i>"판정을 아예 못 읽는다"</i> 가 되고 스프링 기본 본문에는
     * 원인이 없다. 봉투에 코드를 실어 <b>왜 못 읽는지</b>를 남긴다.
     *
     * <p>조용히 건너뛰지 않는 이유는 그것이 <b>집계를 거짓으로 만들기</b> 때문이다 —
     * 리포트의 규칙별 검출 수가 실제보다 적어지고, 그 리포트가 합격 증거로 쓰인다.
     */
    UNKNOWN_FINDING_TYPE(
            500,
            "VERIFICATION-018",
            "검출 행에 알 수 없는 규칙 이름이 있습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    VerificationErrorCode(int status, String code, String message) {
        this.status = status;
        this.code = code;
        this.message = message;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
