package com.kafkick.core.runtimeconfig;

import com.kafkick.core.support.exception.ErrorCode;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.ReasonCode;

import java.util.Optional;

public enum RuntimeConfigErrorCode implements ErrorCode {

    INVALID_REVISION(400, "RUNTIME_CONFIG-001", "설정 revision은 0 이상이어야 합니다."),
    INVALID_UPDATED_BY(400, "RUNTIME_CONFIG-002", "설정 변경자를 입력해야 합니다."),
    REVISION_CONFLICT(409, "RUNTIME_CONFIG-003", "런타임 설정 revision이 충돌했습니다."),
    STORE_UNAVAILABLE(503, "RUNTIME_CONFIG-004", "런타임 설정 저장소를 사용할 수 없습니다.") {
        @Override
        public Dependency dependency() {
            return Dependency.REDIS;
        }
        @Override
        public Optional<ReasonCode> reasonCode() {
            return Optional.of(ReasonCode.TEMPORARILY_UNAVAILABLE);
        }
    },
    READ_ONLY(409, "RUNTIME_CONFIG-005", "읽기 전용 런타임 설정 저장소에서는 변경할 수 없습니다."),
    STORE_CORRUPTED(500, "RUNTIME_CONFIG-006", "런타임 설정 저장소의 값이 손상되었습니다.") {
        @Override
        public Dependency dependency() {
            return Dependency.REDIS;
        }
        @Override
        public Optional<ReasonCode> reasonCode() {
            return Optional.of(ReasonCode.INTERNAL_ERROR);
        }
    },
    STORE_MISSING(500, "RUNTIME_CONFIG-007", "런타임 설정 저장소의 값이 유실되었습니다.") {
        @Override
        public Dependency dependency() {
            return Dependency.REDIS;
        }
        @Override
        public Optional<ReasonCode> reasonCode() {
            return Optional.of(ReasonCode.INTERNAL_ERROR);
        }
    },
    UPDATE_OUTCOME_UNKNOWN(503, "RUNTIME_CONFIG-008", "런타임 설정 변경 결과를 확인할 수 없습니다.") {
        @Override
        public Dependency dependency() {
            return Dependency.REDIS;
        }
        @Override
        public Optional<ReasonCode> reasonCode() {
            return Optional.of(ReasonCode.TEMPORARILY_UNAVAILABLE);
        }
    },
    SERIALIZATION_FAILED(500, "RUNTIME_CONFIG-009", "런타임 설정을 직렬화할 수 없습니다.") {
        @Override
        public Optional<ReasonCode> reasonCode() {
            return Optional.of(ReasonCode.INTERNAL_ERROR);
        }
    };

    private final int status;
    private final String code;
    private final String message;

    RuntimeConfigErrorCode(int status, String code, String message) {
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
