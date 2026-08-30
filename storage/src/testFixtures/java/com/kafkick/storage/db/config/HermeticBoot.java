// 셸 환경변수와 시스템 프로퍼티를 차단한 채 설정 파일만으로 컨텍스트를 띄웁니다.
package com.kafkick.storage.db.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;

/**
 * <b>설정 파일이 무엇으로 해석되는지 보는 테스트는 셸에 좌우되면 안 됩니다.</b>
 *
 * <p>{@link StandardEnvironment} 는 {@code systemProperties} 와 {@code systemEnvironment} 를
 * 붙이고, 이 둘은 설정 파일보다 <b>높습니다.</b> 그래서 셸에
 * {@code SPRING_FLYWAY_ENABLED} 나 {@code SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE} 가 있으면
 * 파일과 무관하게 값이 바뀝니다. 커맨드라인 인자로 로케이션만 고정하는 것으로는 못 막습니다 —
 * <b>단언 대상 프로퍼티 자체</b>의 relaxed 표기가 그대로 들어오기 때문입니다.
 *
 * <p>더 나쁜 쪽은 거짓 빨강이 아니라 <b>거짓 초록</b>입니다. Compose 표기를 셸에 export 해 둔
 * 사람 앞에서는 {@code ---} 를 지워도 통과합니다 — 이 티켓이 존재하는 이유인
 * "조용히 죽는 설정" 이 테스트를 그대로 통과하는 셈입니다.
 *
 * <p>그래서 두 소스를 아예 붙이지 않습니다. 남는 것은 <b>설정 파일과 커맨드라인 인자</b>뿐입니다.
 *
 * <p><b>{@code api}·{@code batch} 가 함께 씁니다.</b> 두 모듈 다 같은 {@code storage.yml} 을
 * 가져다 쓰고 같은 방식으로 조용히 죽으므로, 밀폐 부팅기도 판정기와 같이 한 벌이어야 합니다 —
 * {@link ConfigImportPrecedence} 를 공유한 이유와 같습니다.
 *
 * <p><b>알아 둘 것</b> — {@code user.home} 같은 JDK 표준 프로퍼티도 함께 사라집니다.
 * 지금 두 {@code .example} 의 플레이스홀더는 전부 기본값이 있어 문제가 없지만,
 * {@code ${user.home}} 류가 들어오는 날 이 두 테스트만 "Could not resolve placeholder" 로 죽습니다.
 * 부분 복원은 답이 아닙니다 — {@code ConfigDataEnvironment} 가 설정 파일을 {@code addLast} 로
 * 붙여서, 시스템 프로퍼티를 낮게 되살려도 여전히 설정 파일보다 앞에 옵니다.
 * 그때는 JDK 표준 이름만 골라 <b>다른 이름의 소스</b>로 넣어야 합니다.
 */
public final class HermeticBoot {

    private HermeticBoot() {
    }

    public static ConfigurableApplicationContext run(String... args) {
        StandardEnvironment sealed = new StandardEnvironment() {
            @Override
            protected void customizePropertySources(MutablePropertySources sources) {
                // systemProperties · systemEnvironment 를 붙이지 않는다. 그것이 이 클래스의 전부다.
            }
        };

        ConfigurableApplicationContext context = new SpringApplicationBuilder(EmptyConfig.class)
                .environment(sealed)
                .web(WebApplicationType.NONE)
                .bannerMode(Banner.Mode.OFF)
                .run(args);

        // 단언이 걸리면 호출 측 try-with-resources 는 아직 바인딩되지 않았다 —
        // 리소스 표현식 안에서 던지면 닫을 대상이 없다. 방어가 작동할 때만 컨텍스트가 새는 꼴이라
        // 여기서 닫고 다시 던진다. AssertionError 는 Error 계열이라 Throwable 로 받아야 한다.
        try {
            assertSealed(context);
        } catch (Throwable failure) {
            context.close();
            throw failure;
        }
        return context;
    }

    /**
     * <b>밀폐를 여기서 단언한다.</b> 호출 측 테스트에 맡기면 반쪽만 지켜진다 —
     * JVM 안에서 환경변수는 심을 수 없어서 {@code systemEnvironment} 쪽은 어떤 테스트도 건드리지 못하고,
     * 둘 중 하나만 되살리는 리팩터가 깨끗한 로컬과 CI 에서 전부 초록으로 통과한다.
     *
     * <p>소스 목록을 직접 보면 셸 상태와 무관하게 두 소스를 함께 잡는다.
     */
    private static void assertSealed(ConfigurableApplicationContext context) {
        List<String> names = new ArrayList<>();
        context.getEnvironment().getPropertySources().forEach(source -> names.add(source.getName()));

        assertThat(names)
                .as("밀폐가 깨졌다. 이 소스가 붙으면 설정 파일보다 높아서 셸이 판정을 바꾼다 — "
                        + "그러면 `---` 를 지워도 통과하는 거짓 초록이 난다")
                .doesNotContain(
                        StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME,
                        StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
    }

    /**
     * 자동설정 없는 빈 컨텍스트.
     *
     * <p><b>{@code @SpringBootConfiguration} 을 쓰지 않는다.</b> 그것을 붙이면 이 패키지에
     * 나중에 생길 {@code @SpringBootTest} 가 {@code BatchApplication} 대신 이 빈 클래스를
     * 컨텍스트로 잡는다. 실패가 "DataSource 빈 없음" 으로 나와 원인을 여기서 찾기 어렵다.
     * <p><b>스테레오타입을 아예 붙이지 않는다.</b> {@code @Configuration} 만 붙여도
     * {@code BatchApplication} 이 {@code scanBasePackages = "com.kafkick"} 이라
     * {@code BatchApplicationTests} 의 컴포넌트 스캔에 후보로 걸린다. 지금은 빈이 없어 무해하지만,
     * 다음 사람이 "테스트 전용이니 안전하다" 며 {@code @Bean} 을 하나 넣는 순간
     * 그 스텁이 프로덕션 컨텍스트로 새거나 정의 충돌로 죽는다.
     * {@code SpringApplicationBuilder} 는 primary source 를 직접 등록하므로 애너테이션이 필요 없다.
     */
    static class EmptyConfig {
    }
}
