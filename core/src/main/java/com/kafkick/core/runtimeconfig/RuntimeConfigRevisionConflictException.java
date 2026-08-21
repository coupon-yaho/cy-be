package com.kafkick.core.runtimeconfig;

import com.kafkick.core.support.exception.BusinessException;

public final class RuntimeConfigRevisionConflictException extends BusinessException {

    private final long currentRevision;

    public RuntimeConfigRevisionConflictException(long currentRevision) {
        super(RuntimeConfigErrorCode.REVISION_CONFLICT, "현재 revision: " + currentRevision);
        this.currentRevision = currentRevision;
    }

    public long getCurrentRevision() {
        return currentRevision;
    }
}
