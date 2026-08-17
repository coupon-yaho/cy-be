package com.kafkick.api.admin.observability.dto;

import java.time.Instant;
import java.util.UUID;

import com.kafkick.core.admin.EventType;
import com.kafkick.core.admin.ReasonCode;

/**
 * EventRecorder가 남긴 한 건의 발급 운영 이벤트를 관리자 관제 화면에 전달하는 응답 초안입니다.
 *
 * <p>{@code eventId}는 중복 제거용 UUID이고 {@code occurredAt}은 표시용 시각이므로 cursor 정렬 기준으로
 * 사용하지 않습니다. 이벤트 종류에 따라 발급 식별자, 마스킹 코드, HTTP 상태, 사유, 대기열 순번은 null일
 * 수 있습니다. 회원·쿠폰 식별자는 필수이며 개인정보 원문과 Kafka payload는 포함하지 않습니다.</p>
 *
 * @param eventId 논리 이벤트 중복 제거용 UUID
 * @param eventType EventRecorder 이벤트 유형
 * @param memberId 이벤트 대상 회원 식별자
 * @param couponId 이벤트 대상 쿠폰 캠페인 회차 식별자
 * @param issuanceId 생성된 발급권 식별자; 발급권이 없으면 null
 * @param issuanceCodeMasked 마스킹된 발급 코드; 발급권이 없으면 null
 * @param httpStatus HTTP 결과 상태; 해당하지 않거나 미수집이면 null
 * @param reasonCode 정책·실패 사유
 * @param queueSequence 대기열 순번; 대기열 이벤트가 아니면 null
 * @param replayed 동일 논리 이벤트의 재처리 여부
 * @param occurredAt 이벤트 발생 표시 시각
 */
public record AdminEventItem(UUID eventId, EventType eventType, Long memberId, Long couponId, Long issuanceId,
                             String issuanceCodeMasked, Integer httpStatus, ReasonCode reasonCode, Long queueSequence,
                             boolean replayed, Instant occurredAt) {
    /**
     * 선구축 단계의 JSON 필드 계약을 검증하기 위한 미수집 이벤트 예시를 만듭니다.
     *
     * @param eventId 예시에 사용할 이벤트 UUID
     * @return 선택 필드가 null이고 이벤트 유형이 UNKNOWN인 이벤트 예시
     */
    public static AdminEventItem draft(UUID eventId) { return new AdminEventItem(eventId, EventType.ENTRY_RESULT, 1L, 1L, null, null, null, null, null, false, Instant.EPOCH); }
}
