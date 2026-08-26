package com.kafkick.infra.redis.lifecycle;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("campaign.lifecycle.redis")
public class CampaignLifecycleRedisProperties {

    public static final String DEFAULT_CHANNEL =
            "campaign:lifecycle:closed";

    private String channel = DEFAULT_CHANNEL;

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }
}
