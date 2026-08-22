package com.kafkick.api.support;

import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;

class RequestIdFilterTest {

    private static final String HEADER = "X-Request-Id";

    private final RequestIdFilter filter = new RequestIdFilter();

    @Test
    void preservesSafeClientRequestId() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(HEADER, "client-request_2026.08-22");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(HEADER))
                .isEqualTo("client-request_2026.08-22");
        assertThat(MDC.get("requestId")).isNull();
    }

    @Test
    void replacesRequestIdContainingLogInjectionCharacters() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(HEADER, "trusted\r\nforged-log");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(HEADER))
                .matches("[0-9a-f]{32}");
        assertThat(response.getHeader(HEADER)).doesNotContain("\r", "\n");
    }

    @Test
    void replacesRequestIdLongerThanMaximumLength() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(HEADER, "a".repeat(65));
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(HEADER))
                .matches("[0-9a-f]{32}");
    }
}
