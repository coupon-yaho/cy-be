package com.kafkick.core.runtimeconfig;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuntimeConfigContractTest {

    private static final Instant UPDATED_AT = Instant.parse("2026-08-15T05:02:31.120Z");

    @Test
    void commandRequiresConfigurationAndUpdater() {
        assertThatThrownBy(() -> command(null, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> command(EngineVersion.V3, null, QueueMode.ADAPTIVE, "admin"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> command(EngineVersion.V3, ReleaseStage.V3, null, "admin"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> command(EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, " "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void snapshotRejectsNegativeRevision() {
        assertThatThrownBy(() -> snapshot(-1, UPDATED_AT, "admin", SourceStatus.VALID))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void snapshotRequiresMetadata() {
        assertThatThrownBy(() -> snapshot(0, null, "admin", SourceStatus.VALID))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> snapshot(0, UPDATED_AT, " ", SourceStatus.VALID))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> snapshot(0, UPDATED_AT, "admin", null))
                .isInstanceOf(NullPointerException.class);
    }

    private static RuntimeConfigCommand command(
            EngineVersion engineVersion,
            ReleaseStage releaseStage,
            QueueMode queueMode,
            String updatedBy
    ) {
        return new RuntimeConfigCommand(engineVersion, releaseStage, queueMode, updatedBy);
    }

    private static RuntimeConfigSnapshot snapshot(
            long revision,
            Instant updatedAt,
            String updatedBy,
            SourceStatus status
    ) {
        return new RuntimeConfigSnapshot(
                EngineVersion.V3,
                ReleaseStage.V3,
                QueueMode.ADAPTIVE,
                revision,
                updatedAt,
                updatedBy,
                status
        );
    }
}
