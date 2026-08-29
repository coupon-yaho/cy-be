package com.kafkick.api.observation.datasource;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ClassPathResource;

/**
 * "풀 크기와 마이그레이션 소유권은 모듈이 선언한다" 는 계약이 <b>두 파일에 걸쳐 있어서</b> 여기서 잇는다.
 *
 * <ul>
 *   <li>공유 파일 {@code storage.yml.example} 에 그 키가 <b>없어야</b> 한다
 *   <li>api 의 {@code application.yml.example} 이 그 키를 <b>선언해야</b> 한다
 *   <li>그 선언은 {@code spring.config.import} 를 선언한 문서보다 <b>뒤 문서</b>에 있어야 한다
 * </ul>
 *
 * <p>셋 중 하나만 어긋나도 조용히 무너진다. 공유 파일에 키가 다시 생기면 모듈 선언이 지고,
 * api 선언이 빠지면 Hikari 기본값 10 으로 뜨며, import 를 선언한 문서로 올라가면 공유 파일에
 * 키가 생기는 순간 다시 진다. <b>어느 경우에도 에러나 경고는 없다.</b>
 *
 * <p>실측(CY-179): storage.yml 에 {@code maximum-pool-size: 99} 를 넣었을 때 —
     * 구분자 뒤에 선언한 api 는 3 을 유지했고, 구분자가 없는 batch 는 99 로 떴다.
 *
 * <p>실제 {@code application.yml}·{@code storage.yml} 은 커밋하지 않으므로 커밋되는 템플릿을 본다.
 */
class ConnectionBudgetOwnershipTest {

    private static final String POOL_SIZE = "spring.datasource.hikari.maximum-pool-size";

    private static final String FLYWAY_ENABLED = "spring.flyway.enabled";

    /** 한 줄 전체가 {@code ---} 인 곳만 문서 경계다. 값 안에 든 하이픈에 걸리지 않게 앵커를 건다. */
    private static final Pattern DOCUMENT_SEPARATOR = Pattern.compile("(?m)^---\\s*$");

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

        assertThat(api.getProperty(POOL_SIZE)).isEqualTo("${DB_POOL_SIZE:13}");
        assertThat(api.getProperty(FLYWAY_ENABLED)).isEqualTo("true");
    }

    /**
     * 값이 맞아도 자리가 틀리면 진다. import 로 들어온 문서는 그것을 선언한 문서보다 우선하므로,
     * 같은 문서에 적으면 공유 파일에 같은 키가 생기는 순간 무시된다 — batch 가 지금 그 상태다.
     *
     * <p><b>문자열 위치가 아니라 문서 번호로 본다.</b> 본문을 {@code indexOf} 로 훑으면 주석에 걸린다 —
     * 구분자 앞 주석에 {@code maximum-pool-size} 라는 <b>글자</b>만 있어도 선언은 제자리인데 실패하고
     * (실제로 재현했다), 반대로 주석 처리된 선언을 통과시킨다. 문서마다 따로 파싱하면 주석은 파서가
     * 지우므로 둘 다 사라진다.
     *
     * <p>기준도 "첫 {@code ---} 뒤" 가 아니라 <b>import 를 선언한 문서보다 뒤</b>다. 이기고 지는 관계는
     * 구분자가 아니라 그 선언이 만든다. 문서를 재배치해도 판정이 따라온다.
     */
    @Test
    @DisplayName("api 의 선언은 import 를 선언한 문서보다 뒤에 있다 — 같은 문서면 공유 파일에 진다")
    void apiDeclarationsSitAfterTheImportingDocument() {
        List<Properties> documents = documentsOf("application.yml.example");
        int importing = indexOfDocumentWith(documents, "spring.config.import");

        assertThat(importing).as("spring.config.import 를 선언한 문서가 있어야 한다").isNotNegative();
        assertThat(indexOfDocumentWith(documents, POOL_SIZE))
            .as("풀 크기 선언이 import 문서보다 뒤에 있어야 한다").isGreaterThan(importing);
        assertThat(indexOfDocumentWith(documents, FLYWAY_ENABLED))
            .as("Flyway 선언이 import 문서보다 뒤에 있어야 한다").isGreaterThan(importing);
    }

    /**
     * {@code keySet()} 으로 본다. {@code stringPropertyNames()} 는 값이 String 이 아닌 키를 빼는데,
     * 문서를 하나씩 파싱하면 {@code flyway.enabled: true} 의 값이 Boolean 이라 목록에서 통째로
     * 사라진다(실측: 그러면 이 검사가 {@code -1} 을 받아 <b>파일이 멀쩡한데 실패</b>한다).
     * {@code ManagementConfigTest} 도 같은 함정에 한 번 걸렸는데, 그쪽은 반대로 조용히 통과했다 —
     * 무엇을 찾느냐에 따라 방향이 갈리므로 어느 쪽이든 이 목록을 쓰지 않는다.
     *
     * <p>리스트 키는 {@code spring.config.import[0]} 으로 풀리므로 접두사까지 본다.
     */
    private static int indexOfDocumentWith(List<Properties> documents, String key) {
        for (int index = 0; index < documents.size(); index++) {
            boolean declared = documents.get(index).keySet().stream()
                .map(Object::toString)
                .anyMatch(name -> name.equals(key) || name.startsWith(key + "["));
            if (declared) {
                return index;
            }
        }
        return -1;
    }

    /** 문서 구분자로 잘라 <b>문서마다</b> 파싱한다. 한꺼번에 파싱하면 문서 경계가 사라진다. */
    private static List<Properties> documentsOf(String resource) {
        return DOCUMENT_SEPARATOR.splitAsStream(read(resource))
            .map(ConnectionBudgetOwnershipTest::parseText)
            .toList();
    }

    private static Properties parse(String resource) {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource(resource));
        return yaml.getObject();
    }

    private static Properties parseText(String yamlText) {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ByteArrayResource(yamlText.getBytes(StandardCharsets.UTF_8)));
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
