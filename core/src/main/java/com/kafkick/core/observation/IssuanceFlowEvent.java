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

    /**
     * 미지원 스키마 거부 메시지. <b>상수인 이유는 컨슈머가 이 문구로 원인을 가르기 때문이다.</b>
     *
     * <p>{@code schemaVersion != 1} 이면 역직렬화가 이 예외로 터지고, 컨슈머는 그것을
     * {@code unsupported_schema} 로 분류해 격리 카운터를 올린다. 문구를 리터럴로 양쪽에 적어
     * 두면 여기를 고치는 순간 저쪽 분류가 <b>조용히</b> {@code other} 로 떨어진다 — 이벤트를
     * 잃지는 않지만 배포 직후 무엇이 격리되고 있는지 지표가 말해 주지 못한다.
     */
    public static final String UNSUPPORTED_SCHEMA_MESSAGE = "지원하지 않는 이벤트 스키마 버전입니다.";

    // grade 조회 전 실패해도 이벤트 기록은 중단하지 않는다.
    public IssuanceFlowEvent {
        // 미지원 버전의 역직렬화 실패는 OBS-15가 DLT로 격리한다.
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException(UNSUPPORTED_SCHEMA_MESSAGE);
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

    static IssuanceFlowEvent issueAttempt(UUID eventId, Ctx context) {
        return create(eventId, context, EventType.ISSUE_ATTEMPT, null, null, null,
                null, Dependency.NONE, null, null, context.replayed());
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

        if (eventType == EventType.ISSUE_ATTEMPT) {
            // 결과가 아니라 단계다. 결과 필드가 하나라도 있으면 잘못 만든 이벤트다.
            // replayed 는 위 QUEUE_ADMITTED 와 달리 거부하지 않는다 — 근거는 EventType javadoc.
            if (httpStatus != null || issuanceId != null || issuanceCode != null
                    || reasonCode != null || queuePosition != null || queueSequence != null
                    || dependency != Dependency.NONE) {
                throw new IllegalArgumentException("ISSUE_ATTEMPT 필드 계약을 위반했습니다.");
            }
            requireText(requestId, 36, "requestId");
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

        /**
         * 요청 식별 정보와 실행 설정을 유지하면서 DONE 응답 재사용 여부만 교체합니다.
         *
         * @param replayed 완료된 멱등 응답을 재사용했으면 {@code true}
         * @return replay 여부만 교체한 새로운 Context
         */
        public Ctx withReplayed(boolean replayed) {
            return new Ctx(
                    requestId,
                    memberId,
                    couponId,
                    grade,
                    replayed,
                    occurredAt,
                    engineVersion,
                    releaseStage,
                    queueMode,
                    benchmarkRunId,
                    producerInstanceId
            );
        }
    }
}
