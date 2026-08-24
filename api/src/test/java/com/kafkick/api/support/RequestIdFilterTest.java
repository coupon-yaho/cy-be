package com.kafkick.api.support;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class RequestIdFilterTest {

    private static final String HEADER = "X-Request-Id";
    private static final String MDC_KEY = "requestId";

    private final RequestIdFilter filter = new RequestIdFilter();

    /** 체인 실행 중의 MDC 값. 필터가 심은 값과 응답 헤더가 같은지 보려면 여기서 잡아야 한다. */
    private String mdcInsideChain;

    private MockHttpServletResponse run(String headerValue) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        if (headerValue != null) {
            request.addHeader(HEADER, headerValue);
        }
        MockHttpServletResponse response = new MockHttpServletResponse();
        mdcInsideChain = null;
        filter.doFilter(request, response, (req, res) -> mdcInsideChain = MDC.get(MDC_KEY));
        return response;
    }

    @Test
    @DisplayName("안전한 클라이언트 요청 ID를 그대로 사용한다")
    void preservesSafeClientRequestId() throws Exception {
        assertThat(run("client-request_2026.08-22").getHeader(HEADER))
                .isEqualTo("client-request_2026.08-22");
    }

    @Test
    @DisplayName("로그 인젝션 문자가 포함된 요청 ID는 서버 ID로 교체한다")
    void replacesRequestIdContainingLogInjectionCharacters() throws Exception {
        String unsafe = "trusted\r\nforged-log";

        assertThat(run(unsafe).getHeader(HEADER))
                .isNotEqualTo(unsafe)
                .matches("[0-9a-f]{32}");
    }

    @Test
    @DisplayName("64자를 초과한 요청 ID는 서버 ID로 교체한다")
    void replacesRequestIdLongerThanMaximumLength() throws Exception {
        assertThat(run("a".repeat(65)).getHeader(HEADER))
                .matches("[0-9a-f]{32}");
    }

    @Test
    @DisplayName("헤더가 없으면 서버 ID를 생성한다")
    void generatesWhenAbsent() throws Exception {
        assertThat(run(null).getHeader(HEADER)).matches("[0-9a-f]{32}");
    }

    @Test
    @DisplayName("체인 실행 중 MDC 값이 응답 헤더와 같다")
    void mdcMatchesResponseHeaderDuringChain() throws Exception {
        String requestId = run("client-request_2026.08-22").getHeader(HEADER);

        assertThat(mdcInsideChain).isEqualTo(requestId).isEqualTo("client-request_2026.08-22");
    }

    @Test
    @DisplayName("서버 ID를 만든 경우에도 MDC 값이 응답 헤더와 같다")
    void mdcMatchesGeneratedValue() throws Exception {
        String generated = run("a\tb").getHeader(HEADER);

        assertThat(mdcInsideChain).isEqualTo(generated).matches("[0-9a-f]{32}");
    }

    @Test
    @DisplayName("응답 후 MDC를 비운다")
    void clearsMdc() throws Exception {
        run("client-request_2026.08-22");

        assertThat(MDC.get(MDC_KEY)).isNull();
    }
}
