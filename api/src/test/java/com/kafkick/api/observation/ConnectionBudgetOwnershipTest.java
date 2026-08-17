package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

/**
 * "풀 크기와 마이그레이션 소유권은 모듈이 선언한다" 는 계약이 <b>두 파일에 걸쳐 있어서</b> 여기서 잇는다.
 *
 * <ul>
 *   <li>공유 파일 {@code storage.yml.example} 에 그 키가 <b>없어야</b> 한다
 *   <li>api 의 {@code application.yml.example} 이 그 키를 <b>선언해야</b> 한다
 *   <li>그 선언은 문서 구분자 {@code ---} <b>뒤</b>에 있어야 한다
 * </ul>
 *
 * <p>셋 중 하나만 어긋나도 조용히 무너진다. 공유 파일에 키가 다시 생기면 모듈 선언이 지고,
 * api 선언이 빠지면 Hikari 기본값 10 으로 뜨며, 구분자 앞으로 올라가면 공유 파일에 키가 생기는
 * 순간 다시 진다. <b>어느 경우에도 에러나 경고는 없다.</b>
 *
 * <p>실측(CY-179): storage.yml 에 {@code maximum-pool-size: 99} 를 넣었을 때 —
 * 구분자 뒤에 선언한 api 는 10 을 유지했고, 구분자가 없는 batch 는 99 로 떴다.
 *
 * <p>실제 {@code application.yml}·{@code storage.yml} 은 커밋하지 않으므로 커밋되는 템플릿을 본다.
 */
class ConnectionBudgetOwnershipTest {

    private static final String POOL_SIZE = "spring.datasource.hikari.maximum-pool-size";

    private static final String FLYWAY_ENABLED = "spring.flyway.enabled";

    @Test
    @DisplayName("공유 파일은 모듈별 키를 선언하지 않는다 — 선언하면 모듈 값을 조용히 이긴다")
    void sharedTemplateDeclaresNeitherKey() {
        Properties storage = parse("storage.yml.example");

        assertThat(storage.getProperty(POOL_SIZE)).isNull();
        assertThat(storage.getProperty(FLYWAY_ENABLED)).isNull();
    }

    @Test
    @DisplayName("api 가 자기 풀 크기와 마이그레이션 소유권을 선언한다")
    void apiTemplateOwnsBothKeys() {
        Properties api = parse("application.yml.example");

        assertThat(api.getProperty(POOL_SIZE)).isEqualTo("${DB_POOL_SIZE:10}");
        assertThat(api.getProperty(FLYWAY_ENABLED)).isEqualTo("true");
    }

    /**
     * 값이 맞아도 자리가 틀리면 진다. import 로 들어온 문서는 그것을 선언한 문서보다 우선하므로,
     * 구분자 앞에 있으면 공유 파일에 같은 키가 생기는 순간 무시된다 — batch 가 지금 그 상태다.
     */
    @Test
    @DisplayName("api 의 선언은 --- 뒤에 있다 — 앞에 있으면 공유 파일에 지는 자리다")
    void apiDeclarationsSitAfterTheDocumentSeparator() {
        String text = read("application.yml.example");
        int separator = text.indexOf("\n---");

        assertThat(separator).as("문서 구분자가 있어야 한다").isGreaterThan(0);
        assertThat(text.indexOf("maximum-pool-size")).isGreaterThan(separator);
        assertThat(text.indexOf("flyway:")).isGreaterThan(separator);
    }

    private static Properties parse(String resource) {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource(resource));
        return yaml.getObject();
    }

    private static String read(String resource) {
        try (InputStream in = new ClassPathResource(resource).getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (IOException ex) {
            throw new IllegalStateException(resource + " 를 읽지 못했다", ex);
        }
    }
}
