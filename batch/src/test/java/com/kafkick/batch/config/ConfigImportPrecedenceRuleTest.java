// spring.config.import 의 우선순위 규칙 자체를 못 박는 테스트입니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.storage.db.config.HermeticBoot;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

/**
 * <b>{@code spring.config.import} 로 들어온 문서가 그것을 선언한 문서를 이깁니다.</b>
 * 직관과 반대라서, 실제로 {@code batch/application.yml} 의
 * {@code spring.flyway.enabled: false} 와 {@code maximum-pool-size: 4} 가
 * <b>에러도 경고도 없이</b> storage.yml 값으로 덮여 있었습니다.
 *
 * <p>이 테스트는 우리 설정 파일을 보지 않습니다. Spring 의 규칙만 재현합니다 —
 * 실제 파일을 지키는 것은 {@link BatchConfigPrecedenceTest} 쪽입니다.
 * 둘을 나눈 이유는 <b>실패했을 때 원인이 갈리기 때문</b>입니다.
 * 여기가 깨지면 Boot 가 규칙을 바꾼 것이고, 저기가 깨지면 우리가 키를 잘못 둔 것입니다.
 *
 * <p>자동설정을 켜지 않습니다. DataSource 를 만들 이유가 없고,
 * 이 테스트가 보는 것은 {@link Environment} 하나뿐입니다.
 */
class ConfigImportPrecedenceRuleTest {

    private static final String KEY = "probe.winner";

    @Test
    @DisplayName("같은 문서에 적은 덮어쓰기는 import 된 문서에 진다 — 조용히 무시된다")
    void importedDocumentBeatsDeclaringDocument() {
        assertThat(resolve("classpath:/precedence/import-only.yml"))
                .as("이 값이 declaring-same-document 로 나오면 우선순위가 뒤집힌 것이고, "
                        + "application.yml.example 의 --- 분리는 불필요해진다")
                .isEqualTo("imported");
    }

    @Test
    @DisplayName("--- 로 내린 뒤 문서는 import 된 문서를 이긴다")
    void laterDocumentBeatsImportedDocument() {
        assertThat(resolve("classpath:/precedence/import-then-override.yml"))
                .isEqualTo("declaring-later-document");
    }

    private String resolve(String location) {
        // HermeticBoot 로 띄운다 — 셸에 PROBE_WINNER 가 있으면 이 판정이 뒤집힌다.
        try (ConfigurableApplicationContext context =
                HermeticBoot.run("--spring.config.location=" + location)) {
            return context.getEnvironment().getProperty(KEY);
        }
    }
}
