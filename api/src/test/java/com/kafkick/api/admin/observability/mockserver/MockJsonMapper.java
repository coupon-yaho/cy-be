package com.kafkick.api.admin.observability.mockserver;

import com.fasterxml.jackson.annotation.JsonInclude;

import tools.jackson.databind.json.JsonMapper;

public final class MockJsonMapper {

    private MockJsonMapper() {
    }

    public static JsonMapper create() {
        return JsonMapper.builder()
                .changeDefaultPropertyInclusion(
                        inclusion -> inclusion.withValueInclusion(JsonInclude.Include.NON_NULL))
                .build();
    }
}
