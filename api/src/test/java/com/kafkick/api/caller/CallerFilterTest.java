package com.kafkick.api.caller;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class CallerFilterTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(CallerFilterConfiguration.class)
            .withBean(CallerResolver.class, HeaderCallerResolver::new);

    @Test
    @DisplayName("운영 필터 설정이 CallerFilter를 등록한다")
    void filterConfigurationRegistersFilter() {
        contextRunner.run(context -> assertThat(context).hasSingleBean(CallerFilter.class));
    }

    @Test
    @DisplayName("X-User-Id 가 members.id 형식이면 요청 속성에 심는다")
    void putsCaller() throws Exception {
        assertThat(run("812934")).isEqualTo(new Caller(812934L));
    }

    @Test
    @DisplayName("앞뒤 공백은 흡수한다")
    void trims() throws Exception {
        assertThat(run("  812934  ")).isEqualTo(new Caller(812934L));
    }

    private final CallerFilter filter = new CallerFilter(new HeaderCallerResolver());

    private Object run(String userId) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        if (userId != null) {
            request.addHeader(HeaderCallerResolver.USER_ID_HEADER, userId);
        }
        filter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());
        return request.getAttribute(CallerFilter.ATTRIBUTE);
    }




    @ParameterizedTest
    @ValueSource(strings = {"m_812934", "1 OR 1=1", "0", "-1", "  ", "9999999999999999999999"})
    @DisplayName("members.id 로 파싱되지 않으면 세우지 않는다")
    void rejectsNonNumericId(String userId) throws Exception {
        assertThat(run(userId)).isNull();
    }

    @Test
    @DisplayName("헤더가 없으면 심지 않는다 — 여기서 요청을 막지는 않는다")
    void absentHeaderPassesThrough() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockHttpServletRequest request = new MockHttpServletRequest();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(request.getAttribute(CallerFilter.ATTRIBUTE)).isNull();
        assertThat(response.getStatus()).isEqualTo(200);
    }

}
