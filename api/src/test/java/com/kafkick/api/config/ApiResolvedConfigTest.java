// api 설정 파일을 Boot 로 실제 로드해 바인딩까지 되는지 확인합니다.
package com.kafkick.api.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.kafkick.storage.db.config.HermeticBoot;
import com.kafkick.storage.db.config.ResolvedConfigChecks;
import com.zaxxer.hikari.HikariConfig;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.handler.NoUnboundElementsBindHandler;
import org.springframework.boot.flyway.autoconfigure.FlywayProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * <b>{@link ApiConfigPrecedenceTest} 는 파일을 파싱만 합니다.</b> 그것이 잡는 것은 문법과 중복 키뿐이라
 * <b>Boot 단계에서만 드러나는 것</b>은 전부 통과합니다 — 기본값 없는 플레이스홀더,
 * {@code spring.config.import} 가 없는 파일을 가리키는 경우, Hikari·Flyway 프로퍼티 <b>이름 오타</b>.
 *
 * <p><b>그 이름 오타는 운영 기동을 죽이지 않습니다.</b> {@code @ConfigurationProperties} 의
 * {@code ignoreUnknownFields} 기본값이 {@code true} 라(Boot 4.1.0 바이트코드 확인)
 * 바인딩되는 서브트리({@code spring.flyway}·{@code spring.datasource.hikari})의 모르는 키는
 * <b>예외 없이 무시되고 기본값이 뜹니다.</b> 그래서 compose 가 들어와 기동 스모크 테스트가 생겨도
 * 이 검사를 대체하지 못합니다 — 여기가 유일한 검출 지점입니다.
 *
 * <p>반대로 {@code spring.datasource.url} 처럼 <b>비면 기동이 죽는</b> 키가 있습니다.
 * 그쪽은 값 단언으로 따로 봅니다.
 *
 * <p>하필 api 가 마이그레이션 소유자라 Flyway 키가 가장 위험합니다.
 *
 * <p>{@link HermeticBoot} 로 띄웁니다 — 셸 환경변수가 판정을 바꾸면 이 테스트의 뜻이 없어집니다.
 */
class ApiResolvedConfigTest {

    private static final String LOCATION = "--spring.config.location=classpath:/resolved/application.yml";

    /**
     * {@code HermeticBoot} 의 {@code .web(NONE)} 은 설정 파일의
     * {@code spring.main.web-application-type} 에 진다. api 의 {@code .example} 에 그 키가
     * 들어오는 날 {@code EmptyConfig} 에는 자동설정이 없어 <b>컨텍스트가 아예 못 뜬다.</b>
     * 시끄럽게 죽지만 원인이 이 파일과 무관해 보인다.
     */
    private static final String NO_WEB = "--spring.main.web-application-type=none";

    @Test
    @DisplayName("복사본의 import 경로가 실제로 치환돼 있다")
    void resolvedCopyPointsAtTheResolvedStorage() throws IOException {
        ResolvedConfigChecks.assertCopyPointsAtResolvedStorage(getClass());
    }

    @Test
    @DisplayName("api 설정이 실제로 로드되고 storage.yml 이 붙는다")
    void resolvesAndImportsStorageConfig() {
        try (ConfigurableApplicationContext context = HermeticBoot.run(LOCATION, NO_WEB)) {
            ConfigurableEnvironment environment = context.getEnvironment();

            ResolvedConfigChecks.assertReadsResolvedStorage(environment);
            ResolvedConfigChecks.assertEveryPlaceholderResolves(environment);

            assertThat(environment.getProperty("spring.datasource.url"))
                    .as("import 가 조용히 실패하면 datasource 설정이 통째로 빈다")
                    .contains("rewriteBatchedStatements=true");
            assertThat(environment.getProperty("spring.application.name")).isEqualTo("coupon-api");
        }
    }

    @Test
    @DisplayName("Flyway·Hikari·DataSource 가 실제로 바인딩된다 — 이름 오타를 DB 없이 잡는다")
    void configurationPropertiesBind() {
        try (ConfigurableApplicationContext context = HermeticBoot.run(LOCATION, NO_WEB)) {
            Binder binder = Binder.get(context.getEnvironment());

            // NoUnboundElementsBindHandler 가 필요하다. 기본 Binder 는 모르는 프로퍼티를
            // 조용히 무시해서, 오타를 심어 놓고도 통과한다 — 검증하는 척만 하는 테스트가 된다.
            assertThatCode(() -> binder.bind("spring.flyway", Bindable.of(FlywayProperties.class),
                    new NoUnboundElementsBindHandler(BindHandler.DEFAULT)))
                    .as("api 가 마이그레이션 소유자다. locations·baseline-on-migrate 오타가 가장 위험하다")
                    .doesNotThrowAnyException();

            assertThatCode(() -> binder.bind("spring.datasource.hikari",
                    Bindable.of(HikariConfig.class), new NoUnboundElementsBindHandler(BindHandler.DEFAULT)))
                    .as("이름 오타 하나로 풀 크기가 기본값으로 뜬다. max_connections 배분이 깨진다")
                    .doesNotThrowAnyException();

            // spring.datasource 는 hikari 자식 때문에 NoUnbound 를 못 건다 — 값으로 본다.
            // url·username·password 오타는 필드가 비어 남으므로 여기서 잡힌다.
            DataSourceProperties datasource =
                    binder.bind("spring.datasource", DataSourceProperties.class).get();

            assertThat(datasource.getUrl())
                    .as("이 값이 비면 Boot 가 임베디드 DB 를 찾다 기동에서 죽는다")
                    .contains("rewriteBatchedStatements=true");
            assertThat(datasource.getDriverClassName()).isEqualTo("com.mysql.cj.jdbc.Driver");
            assertThat(datasource.getUsername()).as("이 값이 비면 키 이름이 틀린 것이다").isNotBlank();
            assertThat(datasource.getPassword()).isNotBlank();
        }
    }
}
