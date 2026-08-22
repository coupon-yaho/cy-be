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
    @DisplayName("클라이언트 값 대신 서버 상관 ID를 사용한다")
    void replacesClientValue() throws Exception {
        assertThat(run("abc-123:456").getHeader(HEADER))
                .isNotEqualTo("abc-123:456")
                .matches("[0-9a-f]{32}");
    }

    @Test
    @DisplayName("같은 클라이언트 값을 보내도 요청마다 다른 ID를 만든다")
    void generatesUniqueValueForEachRequest() throws Exception {
        String first = run("client-id").getHeader(HEADER);
        String second = run("client-id").getHeader(HEADER);

        assertThat(first).isNotEqualTo(second);
        assertThat(first).matches("[0-9a-f]{32}");
        assertThat(second).matches("[0-9a-f]{32}");
    }

    @Test
    @DisplayName("헤더가 없으면 새로 만든다")
    void generatesWhenAbsent() throws Exception {
        assertThat(run(null).getHeader(HEADER)).matches("[0-9a-f]{32}");
    }

    @Test
    @DisplayName("체인 실행 중 MDC 값이 응답 헤더와 같다 — 로그와 응답을 같은 키로 묶는 게 목적이다")
    void mdcMatchesResponseHeaderDuringChain() throws Exception {
        String reflected = run("abc-123:456").getHeader(HEADER);

        assertThat(mdcInsideChain).isEqualTo(reflected).matches("[0-9a-f]{32}");
    }

    @Test
    @DisplayName("값을 새로 만든 경우에도 MDC 와 응답 헤더가 같다")
    void mdcMatchesGeneratedValue() throws Exception {
        String generated = run("a\tb").getHeader(HEADER);

        assertThat(mdcInsideChain).isEqualTo(generated).matches("[0-9a-f]{32}");
    }

    @Test
    @DisplayName("응답 후 MDC 를 비운다 — 톰캣 스레드가 재사용된다")
    void clearsMdc() throws Exception {
        run("abc-123");

        assertThat(MDC.get(MDC_KEY)).isNull();
    }
}
