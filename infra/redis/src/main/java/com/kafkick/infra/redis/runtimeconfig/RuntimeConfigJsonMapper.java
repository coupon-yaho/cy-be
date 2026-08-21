package com.kafkick.infra.redis.runtimeconfig;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

final class RuntimeConfigJsonMapper {

    private final ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();

    ObjectMapper objectMapper() {
        return objectMapper;
    }
}
