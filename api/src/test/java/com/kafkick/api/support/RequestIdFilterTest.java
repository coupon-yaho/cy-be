package com.kafkick.api.support;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/** 허용 문자 판정 자체는 {@link HeaderValuesTest} 가 다룬다. 여기서는 필터 동작만 본다. */
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
    @DisplayName("안전한 값은 그대로 응답 헤더로 반사한다")
    void reflectsSafeValue() throws Exception {
        assertThat(run("abc-123:456").getHeader(HEADER)).isEqualTo("abc-123:456");
    }

    @Test
    @DisplayName("위험한 값은 버리고 새로 만든다 — 헤더 반사로 인젝션되면 안 된다")
    void replacesUnsafeValue() throws Exception {
        String unsafe = "a\tb\"c";

        assertThat(run(unsafe).getHeader(HEADER)).isNotEqualTo(unsafe).matches("[0-9a-f]{32}");
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

        assertThat(mdcInsideChain).isEqualTo(reflected).isEqualTo("abc-123:456");
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
