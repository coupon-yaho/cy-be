package com.kafkick.infra.redis.runtimeconfig;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("runtime-config.bootstrap")
public class RuntimeConfigBootstrapProperties {

    private EngineVersion engineVersion = EngineVersion.V1;
    private ReleaseStage releaseStage = ReleaseStage.V1;
    private QueueMode queueMode = QueueMode.OFF;

    public EngineVersion getEngineVersion() {
        return engineVersion;
    }

    public void setEngineVersion(EngineVersion engineVersion) {
        this.engineVersion = engineVersion;
    }

    public ReleaseStage getReleaseStage() {
        return releaseStage;
    }

    public void setReleaseStage(ReleaseStage releaseStage) {
        this.releaseStage = releaseStage;
    }

    public QueueMode getQueueMode() {
        return queueMode;
    }

    public void setQueueMode(QueueMode queueMode) {
        this.queueMode = queueMode;
    }
}
