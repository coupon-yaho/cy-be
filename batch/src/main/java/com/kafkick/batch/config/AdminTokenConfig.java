// 관리자 토큰 관문을 등록하고, 켠 채 토큰이 비면 기동을 거절합니다.
package com.kafkick.batch.config;

import java.util.EnumSet;

import jakarta.servlet.DispatcherType;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.core.support.TimeProvider;

/**
 * <b>손잡이를 둘로 가른다.</b> {@code required} 는 <i>관문을 세울 것인가</i>, {@code token} 은
 * <i>무엇으로 통과시킬 것인가</i>다. 하나로 합쳐 <i>"토큰이 있으면 켠다"</i> 로 두면
 * <b>환경변수를 빠뜨린 배포가 조용히 무방비</b>가 된다 — 그 실패가 정확히 이 관문이 막으려던
 * 상태라 기본값으로 둘 수 없다.
 *
 * <p><b>그래서 기본은 켜짐이고, 켠 채 토큰이 비면 기동을 거절한다.</b> 형제 가드
 * ({@link SchemaPresenceGuard} · {@link DataSourceTimeoutGuard} · {@link DefaultZoneGuard})가
 * 전부 같은 모양이다 — 조용히 넘어가는 것보다 안 뜨는 편이 낫다는 이 저장소의 규약이다.
 *
 * <p><b>끈 상태를 지표로 낸다.</b> 이 스택에는 Loki·promtail 이 없어 <b>로그가 감시 수단이
 * 아니다</b>. 관문이 꺼진 것은 증상이 아예 없어서 — 요청이 그냥 통과한다 — 지표가 유일한
 * 관측 수단이다. 형제 셋도 같은 이유로 같은 모양을 쓴다.
 *
 * <p><b>테스트는 한 곳에서 끈다</b>({@code batch/src/test/resources/application.yml}).
 * 관리자 API 테스트가 여럿인데 각자 토큰을 실으면, 관문을 실수로 지워도 그 테스트들이
 * <b>전부 초록</b>으로 남는다. 관문 자체는 {@code AdminTokenFilterTest} 가 켠 채로 잰다.
 */
@Configuration
public class AdminTokenConfig {

    private static final Logger log = LoggerFactory.getLogger(AdminTokenConfig.class);

    static final String REQUIRED = "batch.admin.auth.required";
    static final String TOKEN = "batch.admin.auth.token";

    /**
     * <b>포트를 밖으로 내보냈는가.</b> 앱은 이것을 스스로 알 수 없어서 배포가 알려 준다
     * ({@code batch-expose.yml} 이 참으로 준다). 알림이 <b>"무방비"</b> 를 판정하는 데 쓴다 —
     * 관문이 꺼진 것만으로는 위험한지 알 수 없고, <b>포트가 열린 채 꺼진 것</b>이 위험이다.
     * 이 신호가 없으면 포트를 안 내보내는 평범한 로컬 스택마다 알림이 울려 곧 무시된다.
     */
    static final String PORT_EXPOSED = "batch.admin.port-exposed";

    /**
     * <b>{@link AdminCorsConfig} 의 {@code CorsFilter} 보다 뒤에 선다.</b> 그래야 이 관문이
     * 401 로 체인을 끊어도 <b>CORS 헤더가 이미 붙어 있어</b> 관제 화면이 그 401 을 읽는다 —
     * 순서가 반대면 거절이 브라우저에 <b>"CORS 오류"</b> 로만 보이고 원인이 토큰이라는 것을
     * 아무도 못 찾는다. 실측으로 그 상태를 확인했다(CY-742 리뷰).
     */
    private static final int ORDER = AdminCorsConfig.ORDER + 100;

    @Bean
    public FilterRegistrationBean<jakarta.servlet.Filter> adminTokenFilter(
            @Value("${" + REQUIRED + ":true}") boolean required,
            @Value("${" + TOKEN + ":}") String token,
            @Value("${" + PORT_EXPOSED + ":false}") boolean portExposed,
            MeterRegistry registry, ObjectMapper objectMapper, TimeProvider timeProvider) {

        Gauge.builder("cy_batch_admin_auth_enforcement", () -> required ? 1 : 0)
                .description("관리자 API 토큰 관문이 켜져 있는가 — 1 켜짐 · 0 꺼짐")
                .register(registry);
        Gauge.builder("cy_batch_admin_port_exposed", () -> portExposed ? 1 : 0)
                .description("업무 포트를 호스트로 내보냈는가 — 1 내보냄 · 0 아님")
                .register(registry);

        FilterRegistrationBean<jakarta.servlet.Filter> registration =
                new FilterRegistrationBean<>();
        if (!required) {
            if (portExposed) {
                log.error("업무 포트를 내보낸 채 관리자 API 토큰 관문이 꺼져 있습니다({}=false). "
                        + "포트에 닿는 누구나 도는 잡을 중단·복구할 수 있습니다 — "
                        + "1차 방어선(포트 미노출)도 없는 상태입니다.", REQUIRED);
            } else {
                log.info("관리자 API 토큰 관문이 꺼져 있습니다({}=false). "
                        + "업무 포트를 안 내보내는 구성이라 1차 방어선(docs/11 §11)이 섭니다.",
                        REQUIRED);
            }
            // **끈 상태에도 필터 인스턴스는 있어야 한다.** FilterRegistrationBean 은
            // setEnabled(false) 여도 등록 시점에 filter 가 null 인지 검사하고, null 이면
            // 톰캣이 "'filter' must not be null" 로 죽어 **컨텍스트가 아예 안 뜬다** —
            // 실측으로 확인했다(테스트 102개가 그 자리에서 죽었다).
            // 통과만 시키는 필터를 둔다. 등록이 꺼져 있어 호출되지도 않는다.
            registration.setFilter((request, response, chain) -> chain.doFilter(request, response));
            registration.setEnabled(false);
            return registration;
        }
        if (token.isBlank()) {
            // 여기서 안 막으면 "켰다고 믿는데 안 켜진" 상태가 된다 — 관문 있는 척이 제일 나쁘다.
            throw new IllegalStateException("관리자 API 토큰 관문이 켜져 있는데 "
                    + TOKEN + " 이 비어 있습니다. 환경변수 BATCH_ADMIN_TOKEN 으로 값을 주거나, "
                    + "일부러 열어 둘 거라면 BATCH_ADMIN_AUTH_REQUIRED=false 로 명시하십시오 "
                    + "(그때는 포트를 밖으로 내보내면 안 됩니다).");
        }

        log.info("관리자 API 토큰 관문을 켰습니다. 헤더={}", AdminTokenFilter.HEADER);
        registration.setFilter(new AdminTokenFilter(token, objectMapper, timeProvider));
        registration.addUrlPatterns("/api/v1/admin/*");
        // **스코프가 이제 이 한 줄에 걸려 있다.** 필터 안에서 경로를 다시 보던 것을 걷어냈으니
        // (원문 URI 라 인코딩 우회가 났다), "어떤 디스패치로 들어오든 이 매핑이 걸리는가" 가
        // 곧 방어 범위다.
        //
        // ⚠️ **지금은 안 적어도 같은 값이다.** AbstractFilterRegistrationBean.determineDispatcherTypes
        //    가 비어 있으면 필터가 OncePerRequestFilter 인지 보고 그때 allOf 를 준다
        //    (spring-boot-4.1.0 바이트코드로 확인). 그래도 적는 이유는 그 기본값이
        //    **상속 관계에 매여 있어서**다 — 나중에 이 필터가 OncePerRequestFilter 를 안
        //    물려받게 바뀌면 기본이 REQUEST 로 좁아지고, 포워드·에러 디스패치로 들어오는
        //    길이 조용히 열린다. theScopeCoversEveryDispatch 가 이 값을 단언한다.
        registration.setDispatcherTypes(EnumSet.allOf(DispatcherType.class));
        registration.setOrder(ORDER);
        return registration;
    }
}
