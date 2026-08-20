package com.kafkick.core.observation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.kafkick.core.member.Grade;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record IssuanceFlowEvent(
        int schemaVersion,
        UUID eventId,
        EventType eventType,
        String requestId,
        long memberId,
        long couponId,
        Long issuanceId,
        String issuanceCode,
        Grade grade,
        Integer httpStatus,
        ReasonCode reasonCode,
        Dependency dependency,
        Long queuePosition,
        Long queueSequence,
        boolean replayed,
        Instant occurredAt,
        EngineVersion engineVersion,
        ReleaseStage releaseStage,
        QueueMode queueMode,
        Long benchmarkRunId,
        String producerInstanceId
) {

    public static final int CURRENT_SCHEMA_VERSION = 1;

    // grade 조회 전 실패해도 이벤트 기록은 중단하지 않는다.
    public IssuanceFlowEvent {
        // 미지원 버전의 역직렬화 실패는 OBS-15가 DLT로 격리한다.
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("지원하지 않는 이벤트 스키마 버전입니다.");
        }
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(dependency, "dependency");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(engineVersion, "engineVersion");
        Objects.requireNonNull(releaseStage, "releaseStage");
        Objects.requireNonNull(queueMode, "queueMode");
        requirePositive(memberId, "memberId");
        requirePositive(couponId, "couponId");
        requireText(producerInstanceId, 100, "producerInstanceId");
        if (queueSequence != null && queueSequence < 0) {
            throw new IllegalArgumentException("queueSequence는 0 이상이어야 합니다.");
        }
        if (queuePosition != null && queuePosition < 1) {
            throw new IllegalArgumentException("queuePosition은 1 이상이어야 합니다.");
        }
        if (issuanceId != null) {
            requirePositive(issuanceId, "issuanceId");
        }
        if (issuanceCode != null) {
            requireText(issuanceCode, 16, "issuanceCode");
        }
        validateByEventType(eventType, requestId, issuanceId, issuanceCode, httpStatus,
                reasonCode, dependency, queuePosition, queueSequence, replayed);
    }

    static IssuanceFlowEvent issued(
            UUID eventId,
            Ctx context,
            long issuanceId,
            String issuanceCode
    ) {
        requirePositive(issuanceId, "issuanceId");
        requireText(issuanceCode, 16, "issuanceCode");
        return create(eventId, context, EventType.ISSUE_RESULT, 201, issuanceId, issuanceCode,
                null, Dependency.NONE, null, null, context.replayed());
    }

    static IssuanceFlowEvent issueRejected(
            UUID eventId,
            Ctx context,
            int httpStatus,
            ReasonCode reasonCode,
            Dependency dependency
    ) {
        return create(eventId, context, EventType.ISSUE_RESULT, httpStatus, null, null,
                reasonCode, dependency, null, null, context.replayed());
    }

    static IssuanceFlowEvent entry(
            UUID eventId,
            Ctx context,
            int httpStatus,
            ReasonCode reasonCode,
            Dependency dependency,
            Long queuePosition,
            Long queueSequence
    ) {
        return create(eventId, context, EventType.ENTRY_RESULT, httpStatus, null, null,
                reasonCode, dependency, queuePosition, queueSequence, context.replayed());
    }

    static IssuanceFlowEvent admitted(UUID eventId, Ctx context, long queueSequence) {
        if (queueSequence < 0) {
            throw new IllegalArgumentException("queueSequence는 0 이상이어야 합니다.");
        }
        return create(eventId, context, EventType.QUEUE_ADMITTED, null, null, null,
                null, Dependency.NONE, null, queueSequence, context.replayed());
    }

    private static IssuanceFlowEvent create(
            UUID eventId,
            Ctx context,
            EventType eventType,
            Integer httpStatus,
            Long issuanceId,
            String issuanceCode,
            ReasonCode reasonCode,
            Dependency dependency,
            Long queuePosition,
            Long queueSequence,
            boolean replayed
    ) {
        Objects.requireNonNull(context, "context");
        return new IssuanceFlowEvent(
                CURRENT_SCHEMA_VERSION,
                eventId,
                eventType,
                context.requestId(),
                context.memberId(),
                context.couponId(),
                issuanceId,
                issuanceCode,
                context.grade(),
                httpStatus,
                reasonCode,
                dependency,
                queuePosition,
                queueSequence,
                replayed,
                context.occurredAt(),
                context.engineVersion(),
                context.releaseStage(),
                context.queueMode(),
                context.benchmarkRunId(),
                context.producerInstanceId()
        );
    }

    private static void validateByEventType(
            EventType eventType,
            String requestId,
            Long issuanceId,
            String issuanceCode,
            Integer httpStatus,
            ReasonCode reasonCode,
            Dependency dependency,
            Long queuePosition,
            Long queueSequence,
            boolean replayed
    ) {
        if (eventType == EventType.QUEUE_ADMITTED) {
            if (httpStatus != null || issuanceId != null || issuanceCode != null || reasonCode != null
                    || queuePosition != null || dependency != Dependency.NONE || replayed) {
                throw new IllegalArgumentException("QUEUE_ADMITTED 필드 계약을 위반했습니다.");
            }
            Objects.requireNonNull(queueSequence, "queueSequence");
            return;
        }

        requireText(requestId, 36, "requestId");
        validateHttpResult(httpStatus, reasonCode);
        if (eventType == EventType.ENTRY_RESULT) {
            if (issuanceId != null || issuanceCode != null) {
                throw new IllegalArgumentException("ENTRY_RESULT에는 발급 식별자를 넣을 수 없습니다.");
            }
            if (httpStatus < 400 && httpStatus != 200 && httpStatus != 202) {
                throw new IllegalArgumentException("성공 ENTRY_RESULT의 HTTP 상태는 200 또는 202여야 합니다.");
            }
            if (httpStatus == 202 && (queuePosition == null || queueSequence == null)) {
                throw new IllegalArgumentException("202 ENTRY_RESULT에는 대기열 순번이 필요합니다.");
            }
            if (httpStatus != 202 && (queuePosition != null || queueSequence != null)) {
                throw new IllegalArgumentException("202가 아닌 ENTRY_RESULT에는 대기열 순번을 넣을 수 없습니다.");
            }
            return;
        }

        if (queuePosition != null || queueSequence != null) {
            throw new IllegalArgumentException("ISSUE_RESULT에는 대기열 순번을 넣을 수 없습니다.");
        }
        if (httpStatus < 400 && httpStatus != 201) {
            throw new IllegalArgumentException("성공 ISSUE_RESULT의 HTTP 상태는 201이어야 합니다.");
        }
        if (httpStatus == 201 && (issuanceId == null || issuanceCode == null)) {
            throw new IllegalArgumentException("201 ISSUE_RESULT에는 발급 식별자가 필요합니다.");
        }
        if (httpStatus != 201 && (issuanceId != null || issuanceCode != null)) {
            throw new IllegalArgumentException("201이 아닌 ISSUE_RESULT에는 발급 식별자를 넣을 수 없습니다.");
        }
    }

    private static void validateHttpResult(Integer httpStatus, ReasonCode reasonCode) {
        if (httpStatus == null || httpStatus < 100 || httpStatus > 599) {
            throw new IllegalArgumentException("HTTP 상태는 100~599여야 합니다.");
        }
        if (httpStatus >= 400 && reasonCode == null) {
            throw new IllegalArgumentException("실패 HTTP 이벤트에는 reasonCode가 필요합니다.");
        }
        if (httpStatus < 400 && reasonCode != null) {
            throw new IllegalArgumentException("성공 HTTP 이벤트에는 reasonCode를 넣을 수 없습니다.");
        }
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + "는 양수여야 합니다.");
        }
    }

    private static void requireText(String value, int maxLength, String name) {
        if (value == null || value.isBlank() || value.length() > maxLength) {
            throw new IllegalArgumentException(name + " 형식이 올바르지 않습니다.");
        }
    }

    public record Ctx(
            String requestId,
            long memberId,
            long couponId,
            Grade grade,
            boolean replayed,
            Instant occurredAt,
            EngineVersion engineVersion,
            ReleaseStage releaseStage,
            QueueMode queueMode,
            Long benchmarkRunId,
            String producerInstanceId
    ) {

        /**
         * 요청 식별 정보와 실행 설정을 유지하면서 이벤트 발생 시각만 교체합니다.
         *
         * <p>요청 시작 시 구성한 Context를 결과 이벤트에 사용할 때 시작 시각이 결과 시각으로
         * 기록되지 않도록, 실제 결과가 확정된 시각을 반영하는 데 사용합니다.
         *
         * @param occurredAt 실제 관측 결과가 발생한 시각
         * @return 발생 시각만 교체한 새로운 Context
         */
        public Ctx withOccurredAt(Instant occurredAt) {
            return new Ctx(
                    requestId,
                    memberId,
                    couponId,
                    grade,
                    replayed,
                    Objects.requireNonNull(occurredAt, "occurredAt"),
                    engineVersion,
                    releaseStage,
                    queueMode,
                    benchmarkRunId,
                    producerInstanceId
            );
        }
    }
}
