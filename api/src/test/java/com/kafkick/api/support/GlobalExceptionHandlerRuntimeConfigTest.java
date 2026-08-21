package com.kafkick.api.support;

import com.kafkick.api.admin.support.AdminJsonTest;
import com.kafkick.core.runtimeconfig.RuntimeConfigErrorCode;
import com.kafkick.core.runtimeconfig.RuntimeConfigRevisionConflictException;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletRequest;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

@AdminJsonTest
class GlobalExceptionHandlerRuntimeConfigTest {

    private static final Instant NOW = Instant.parse("2026-08-21T00:00:00Z");

    private final ObjectMapper objectMapper;

    @Autowired
    GlobalExceptionHandlerRuntimeConfigTest(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Test
    void ordinaryBusinessExceptionOmitsCurrentRevision() throws Exception {
        ErrorResponse error = handle(new BusinessException(RuntimeConfigErrorCode.INVALID_REVISION));

        assertThat(error.currentRevision()).isNull();
        assertThat(objectMapper.writeValueAsString(error)).doesNotContain("currentRevision");
    }

    @Test
    void revisionConflictIncludesOnlyItsActualCurrentRevision() throws Exception {
        ErrorResponse error = handle(new RuntimeConfigRevisionConflictException(17));

        assertThat(error.currentRevision()).isEqualTo(17L);
        assertThat(objectMapper.writeValueAsString(error)).contains("\"currentRevision\":17");
    }

    private ErrorResponse handle(BusinessException exception) {
        GlobalExceptionHandler handler = new GlobalExceptionHandler(
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)));
        return handler.handleBusinessException(exception, new MockHttpServletRequest())
                .getBody()
                .error();
    }
}
