package com.kafkick.core.observation.attempt;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.ReasonCode;

/**
 * live 화면에 실리는 한 건. <b>정제는 여기서 끝난다 — Redis 에 들어가는 것이 이 모양이다.</b>
 *
 * <p>{@link IssuanceFlowEvent} 를 그대로 XADD 하지 않는 이유는 그 안에 화면에 나가면 안 되는
 * 값이 있기 때문이다. 정제를 <b>읽는 쪽</b>에 두면 이미 Redis 에 원문이 앉은 뒤라 늦는다 —
 * 관리자 응답에서만 빠질 뿐이고, Redis 를 직접 들여다보면 그대로 보인다. 그래서 쓰기 직전에
 * 자른다.
 *
 * <table>
 *   <caption>싣지 않는 것</caption>
 *   <tr><th>필드</th><th>이유</th></tr>
 *   <tr><td>{@code requestId}</td><td>요청 추적자. 화면이 쓰지 않고, 로그와 교차하면 개별 요청이 특정된다</td></tr>
 *   <tr><td>{@code producerInstanceId}</td><td>발신 인스턴스. 화면의 관심사가 아니다</td></tr>
 *   <tr><td>{@code issuanceCode} 전문</td><td>쿠폰 코드다. 앞 8자만 싣는다</td></tr>
 *   <tr><td>{@code benchmarkRunId}·{@code engineVersion}·{@code releaseStage}·{@code queueMode}</td>
 *       <td>회차 메타데이터. 이 화면이 아니라 회차 API 가 갖는다</td></tr>
 *   <tr><td>{@code dependency}</td><td>실패 원인 분해는 Prometheus 쪽 패널이 한다</td></tr>
 * </table>
 *
 * <p>이름·이메일·전화·JWT·Entry-Token 은 애초에 {@link IssuanceFlowEvent} 에 없다. 여기서
 * 빼는 것이 아니라 <b>이벤트 계약이 이미 담지 않는다</b> — 그 사실에 기대는 것이 아니라
 * 아래 화이트리스트가 그 성질을 이 자리에서 다시 못박는다.
 *
 * <p>{@code memberId} 는 남긴다. {@code members.id} 라 그 자체로는 사람을 가리키지 않고,
 * 화면이 "같은 사람이 반복 실패 중" 을 보는 유일한 수단이다.
 *
 * @param eventId 중복 제거용 UUID
 * @param eventType 관측 단계
 * @param memberId 회원 식별자
 * @param couponId 캠페인 회차 식별자
 * @param issuanceId 발급권 식별자. 201 ISSUE_RESULT 에만 있다
 * @param issuanceCodeMasked 발급 코드 앞 8자. 발급권이 없으면 null
 * @param grade 회원 등급. 등급 조회 전 실패했으면 null
 * @param httpStatus HTTP 결과. QUEUE_ADMITTED · ISSUE_ATTEMPT 에는 없다
 * @param reasonCode 실패·거절 사유. 미매핑은 유실이 아니라 UNMAPPED 로 온다
 * @param queuePosition 안내한 대기 위치
 * @param queueSequence 대기열 내부 처리 순서
 * @param replayed 저장된 결과를 다시 내보낸 건인지
 * @param occurredAt 프로듀서 시계. 정렬은 근사다
 * @param ingestedAt 컨슈머 도착 시각
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record AttemptLiveEntry(
        UUID eventId,
        EventType eventType,
        long memberId,
        long couponId,
        Long issuanceId,
        String issuanceCodeMasked,
        Grade grade,
        Integer httpStatus,
        ReasonCode reasonCode,
        Long queuePosition,
        Long queueSequence,
        boolean replayed,
        Instant occurredAt,
        Instant ingestedAt
) {

    /** 쿠폰 코드에서 화면에 내보내는 길이. 계약은 {@code char(16)} 이고 그 절반만 낸다. */
    public static final int CODE_PREFIX_LENGTH = 8;

    public AttemptLiveEntry {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(ingestedAt, "ingestedAt");
    }

    /**
     * 도착한 한 건을 화면용으로 정제한다.
     *
     * <p><b>화이트리스트다.</b> 옮길 필드를 하나씩 적는다 — 복사 후 제거로 만들면 이벤트 계약에
     * 필드가 느는 순간 그것이 <b>자동으로</b> 화면과 Redis 로 나간다. 새 필드를 내보내려면
     * 여기를 고쳐야 하고, 그 자리가 리뷰에 걸린다.
     *
     * @param record 컨슈머가 받은 한 건
     * @return 정제본
     */
    public static AttemptLiveEntry from(AttemptRecord record) {
        Objects.requireNonNull(record, "record");
        IssuanceFlowEvent event = record.event();
        return new AttemptLiveEntry(
                event.eventId(),
                event.eventType(),
                event.memberId(),
                event.couponId(),
                event.issuanceId(),
                maskIssuanceCode(event.issuanceCode()),
                event.grade(),
                event.httpStatus(),
                event.reasonCode(),
                event.queuePosition(),
                event.queueSequence(),
                event.replayed(),
                event.occurredAt(),
                record.ingestedAt());
    }

    /**
     * 쿠폰 코드를 앞 {@value #CODE_PREFIX_LENGTH} 자로 자른다.
     *
     * <p>계약상 {@code requireText(issuanceCode, 16, ...)} 이라 16 자 이하만 온다. 그보다 짧은
     * 값이 오면 자를 것이 없으므로 그대로 둔다 — {@code substring} 을 조건 없이 부르면
     * {@code StringIndexOutOfBoundsException} 이고, 그 예외는 발급이 아니라 화면을 죽인다.
     */
    private static String maskIssuanceCode(String issuanceCode) {
        if (issuanceCode == null) {
            return null;
        }
        return issuanceCode.length() <= CODE_PREFIX_LENGTH
                ? issuanceCode
                : issuanceCode.substring(0, CODE_PREFIX_LENGTH);
    }
}
