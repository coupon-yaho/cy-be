// spring.config.import 로 가져온 설정이 모듈 설정을 조용히 덮는 자리를 잡습니다.
package com.kafkick.storage.db.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Assertions;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.Yaml;

/**
 * <b>{@code spring.config.import} 로 들어온 설정이 그것을 선언한 문서를 이깁니다.</b> 직관과 반대라서,
 * 선언 문서에 같은 키를 적으면 <b>에러도 경고도 없이</b> 가져온 값으로 덮입니다.
 * 실제로 batch 의 {@code spring.flyway.enabled: false} 와 {@code maximum-pool-size: 4} 가
 * 그렇게 죽어 있었고, 관측 담당자가 {@code Environment} 를 찍어 보고서야 드러났습니다.
 *
 * <p>덮어쓰려면 같은 파일 안에서 {@code ---} 로 문서를 갈라 <b>뒤 문서</b>에 둬야 합니다.
 *
 * <p><b>왜 storage 픽스처인가</b> — {@code api} 와 {@code batch} 가 같은 {@code storage.yml} 을
 * 가져다 씁니다. 사고 구조가 같으므로 판정도 한 벌이어야 합니다. 모듈마다 복사하면
 * 한쪽만 고쳐지고 다른 쪽은 낡은 규칙으로 통과합니다.
 *
 * <p><b>{@code .example} 을 읽습니다.</b> 실제 {@code application.yml}·{@code storage.yml} 은
 * gitignore 대상이라 사람마다 다르고 CI 에는 아예 없습니다. 저장소가 지킬 수 있는 것은
 * {@code .example} 뿐이고, 그것이 두 파일을 같이 고쳐야 하는 이유이기도 합니다.
 */
public final class ConfigImportPrecedence {

    /**
     * <b>Boot 가 {@code Map} 으로 바인딩하는 루트.</b> 이 아래에서는 엔트리가 소스별로 합쳐지므로
     * 조상 관계가 곧 덮어쓰기가 아니다 — {@code logging.level.com} 과
     * {@code logging.level.com.kafkick} 은 둘 다 산다.
     *
     * <p>이 목록의 원소는 <b>가드가 아니라 검사를 끄는 예외</b>다. 그래서 저장소의 두 yml 에
     * 실제로 있는 것만 둔다. 앞으로 쓸 법한 루트까지 미리 넣으면 아무도 안 쓰는 자리의 검사가
     * 조용히 꺼진 채로 남는다 — {@code spring.config.activate.*} 허용목록을 지운 것과 같은 이유다.
     * 새 Map 루트가 필요해지는 날은 이 검사가 빨개지는 날이고, 그때 늘리면 된다.
     *
     * <p>반대로 넣지 않으면 정당한 키를 충돌로 오판하고, 실패 메시지가
     * <i>"{@code ---} 아래로 내려라"</i> 라고 <b>반대로 안내한다.</b> 그러면 그 키는 옮겨져도 효과가 없고
     * 뒤 문서라 충돌 검사 밖에 영구히 남는다. 그래서 실패 메시지에도 이 경우를 적어 둔다.
     */
    private static final List<ConfigurationPropertyName> MERGED_MAP_ROOTS = List.of(
            ConfigurationPropertyName.of("logging.level"),
            ConfigurationPropertyName.of("spring.jpa.properties"));

    /** 첫 숫자 인덱스. {@code a[0]}·{@code a[1]}·리스트 잎 {@code a} 를 한 컬렉션으로 묶는다. */
    private static final Pattern NUMERIC_INDEX = Pattern.compile("\\[\\d+]");

    private static final String IMPORT_KEY = "spring.config.import";

    private final String moduleResource;
    private final String importedResource;
    private final List<Map<String, Object>> documents;
    private final Set<String> importedKeys;
    private final int declaringIndex;

    private ConfigImportPrecedence(Class<?> loader, String moduleResource, String importedResource) {
        this.moduleResource = moduleResource;
        this.importedResource = importedResource;
        this.documents = load(loader, moduleResource);
        this.importedKeys = flatten(single(load(loader, importedResource))).keySet();
        this.declaringIndex = indexOfImportDeclaration();
    }

    /**
     * <b>대조할 import 대상을 생성 시점에 못 박는다.</b> 별도 빌더 단계로 두면 호출 측이 빠뜨릴 수
     * 있는데, 그건 검사 전체의 전제라 선택이면 안 된다 — 대상이 하나 늘면 그 파일이 정의하는 키와
     * 선언 문서의 충돌을 <b>아무도 보지 않게 된다.</b>
     *
     * @param loader           리소스를 찾을 클래스. 호출한 모듈의 테스트 클래스패스를 쓴다
     * @param moduleResource   모듈 설정. 예 {@code "/application.yml.example"}
     * @param importedResource 가져오는 설정. 예 {@code "/storage.yml.example"}
     * @param expectedImports  {@code spring.config.import} 가 가리켜야 하는 위치 전부
     */
    public static ConfigImportPrecedence of(Class<?> loader, String moduleResource,
            String importedResource, String... expectedImports) {
        // getClassLoader().getResources() 는 항상 클래스패스 루트 기준이다. 슬래시를 빼면
        // substring(1) 이 첫 글자를 먹어 0개가 되고, 실패 메시지가 runtimeOnly 를 의심하게 만든다.
        for (String resource : List.of(moduleResource, importedResource)) {
            assertThat(resource)
                    .as("리소스 경로는 클래스패스 루트 기준 절대 경로여야 한다")
                    .startsWith("/");
        }

        ConfigImportPrecedence precedence =
                new ConfigImportPrecedence(loader, moduleResource, importedResource);

        assertThat(precedence.importedLocations())
                .as("import 대상이 늘면 대조할 파일도 늘어야 한다. "
                        + "지금 대조하는 것은 " + importedResource + " 하나뿐이라, "
                        + "새 파일이 정의하는 키와 선언 문서의 충돌은 아무도 보지 않는다")
                .containsExactlyElementsOf(Arrays.asList(expectedImports));
        return precedence;
    }

    /**
     * 모듈이 무조건 받아야 하는 검사 전부.
     *
     * <p><b>호출 측이 하나를 빠뜨리는 것을 막는다.</b> 실제로 api 는 {@code ---} 가 없다는 이유로
     * 뒤 문서 검사를 안 불렀는데, {@code ---} 를 만드는 날 그 문서 전체가 검사 밖으로 빠진다.
     * 문서가 하나면 공허하게 통과할 뿐이라 지금 불러 두는 것이 옳다.
     *
     * <p>이름을 {@code assertAll} 로 두지 않는다 — JUnit 의 그것과 헷갈린다.
     * 동작은 그쪽에 맞춰 두 실패를 함께 보고한다.
     *
     * <p>두 단언은 {@code private} 이다. 공개하면 호출 측이 다시 부분 집합만 고를 수 있는데,
     * 그것이 이 메서드를 만들게 한 버그다.
     */
    public void assertEveryRule() {
        // JUnit 의 assertAll 로 위임한다 — 첫 실패에서 멈추면 설정 파일에 문제가 둘일 때
        // 앞엣것만 보이고, 고쳐서 다시 돌려야 뒤엣것이 드러난다.
        Assertions.assertAll(this::assertNoSilentOverride, this::assertOverrideDocumentIsPure);
    }

    /** import 를 선언한 문서까지는 가져온 설정과 겹치는 키가 하나도 없어야 한다. */
    private void assertNoSilentOverride() {
        Set<String> collisions = new LinkedHashSet<>();
        for (int i = 0; i <= declaringIndex; i++) {
            for (String key : flatten(documents.get(i)).keySet()) {
                if (overlaps(key, importedKeys)) {
                    collisions.add(key);
                }
            }
        }

        assertThat(collisions)
                .as("이 키들은 " + importedResource + " 값으로 조용히 덮인다. "
                        + "`---` 아래 문서로 내려야 한다. 덮어쓸 생각이 없었다면 지우는 것이 맞다 — "
                        + "남겨 두면 다음 사람이 적힌 값이 뜬다고 믿는다. "
                        + "(" + moduleResource + " 에 `---` 가 아직 없다면 그것부터 만들어야 한다.) "
                        + "다만 이 키가 Boot 의 Map 바인딩 대상이면 얘기가 다르다 — "
                        + "그때는 `---` 로 내리는 것이 아니라 MERGED_MAP_ROOTS 에 루트를 추가해야 한다. "
                        + "엔트리는 소스별로 합쳐지므로 애초에 덮어쓰기가 아니다")
                .isEmpty();
    }

    /**
     * {@code ---} 아래 문서에는 덮어쓰는 키만 있어야 한다.
     *
     * <p>충돌 검사는 앞 문서만 훑는다. 무관한 키가 뒤로 새면 <b>검사 범위가 조용히 줄어든다</b> —
     * 나중에 가져온 설정에 같은 키가 생겨도 아무도 모른다.
     */
    private void assertOverrideDocumentIsPure() {
        Set<String> unexpected = new LinkedHashSet<>(keysAfterImport());
        unexpected.removeIf(key -> overlaps(key, importedKeys));

        assertThat(unexpected)
                .as("덮어쓸 것이 아닌 키가 `---` 아래에 있다. 앞 문서로 올려야 충돌 검사를 받는다. "
                        + "다만 spring.config.activate.* 라면 얘기가 다르다 — 그 키는 뒤 문서에만 둘 수 있고, "
                        + "붙는 순간 문서가 통째로 비활성화될 수 있다. "
                        + "예외로 뚫지 말고 그때 이 검사의 전제부터 다시 봐라")
                .isEmpty();
    }

    /**
     * <b>공유 파일이 모듈별 키를 정의하지 않는다.</b> {@link #assertOverridden} 의 반대다.
     *
     * <p>{@code spring.config.import} 로 들어온 문서는 선언 문서를 <b>이긴다</b>. 그래서
     * 공유 파일이 모듈마다 달라야 하는 값(풀 크기 · 마이그레이션 소유자)을 정의하면,
     * 각 모듈이 자기 파일에 적은 값이 <b>에러도 경고도 없이 무시된다.</b> 실제로 그랬다 —
     * batch 의 {@code maximum-pool-size: 4} 가 무시되고 10 으로 떴다.
     *
     * <p>지금은 공유 파일이 그 키들을 안 갖는다. 이 검사는 <b>그 상태를 계약으로 고정</b>한다 —
     * 누가 편의로 되돌려 놓으면 그 순간 빨개진다. 조용히 무시되는 쪽으로는 안 돌아간다.
     */
    public void assertNotDefinedByImport(String... keys) {
        for (String key : keys) {
            assertThat(importedKeys.stream().anyMatch(raw -> sameProperty(raw, key)))
                    .as(importedResource + " 이 " + key + " 를 정의하면 각 모듈이 선언한 값이 "
                            + "조용히 무시된다 — import 문서가 선언 문서를 이긴다. "
                            + "그 키는 모듈마다 달라야 하므로 공유 파일에 두지 않는다")
                    .isFalse();
        }
    }

    /**
     * 지정한 키가 실제로 덮어쓰기인지 — 뒤 문서에도 있고 가져온 설정에도 있는지 확인한다.
     *
     * <p><b>{@link #overlaps} 를 쓰지 않는다.</b> 질문이 다르다 — {@code overlaps} 는
     * *"같은 트리를 건드리는가"* 라 조상·자손도 참이고, 여기는 *"그 프로퍼티가 살아 있는가"* 다.
     * {@code overlaps} 로 판정하면 {@code hikari:} 헤더만 남기고 자식을 주석 처리해도
     * {@code hikari.maximum-pool-size} 가 있다고 통과한다.
     *
     * <p>다만 정규화 규칙은 그쪽과 같은 것을 쓴다. 이 메서드만 원문으로 비교하다가
     * relaxed binding 을 잃은 적이 있고, 되살릴 때는 컬렉션 루트 자르기를 한쪽에만 걸었고,
     * Map 루트에서 원문을 봐야 한다는 규칙도 놓쳤다 — 세 번 다 실패 메시지가 <b>사실의 반대</b>를 말했다.
     */
    public void assertOverridden(String... keys) {
        Set<String> later = keysAfterImport();

        for (String key : keys) {
            assertThat(later.stream().anyMatch(raw -> sameProperty(raw, key)))
                    .as(key + " 는 `---` 아래에서만 산다. 뒤 문서 키=" + later)
                    .isTrue();

            assertThat(importedKeys.stream().anyMatch(raw -> sameProperty(raw, key)))
                    .as(importedResource + " 이 " + key + " 를 더 이상 정의하지 않으면 "
                            + "`---` 분리의 근거가 사라진다. 그때는 이 검사가 아니라 설정 구조를 다시 봐야 한다")
                    .isTrue();
        }
    }

    /**
     * <b>같은 프로퍼티인가.</b> {@link #overlaps} 와 정규화 규칙은 같지만 조상 관계는 보지 않는다 —
     * 그쪽은 "덮어쓰기가 일어나는가", 여기는 "그 키가 실재하는가" 를 묻는다.
     */
    static boolean sameProperty(String rawKey, String requested) {
        if (isUnderMergedMapRoot(rawKey) || isUnderMergedMapRoot(requested)) {
            // Map 루트 아래는 이름이 원문 그대로 전달되므로 정규화하지 않는다.
            return isUnderMergedMapRoot(rawKey) && isUnderMergedMapRoot(requested)
                    && collectionRoot(rawKey).equals(collectionRoot(requested));
        }
        return normalizedCollectionRoot(rawKey).equals(normalizedCollectionRoot(requested));
    }

    /**
     * <b>같은 프로퍼티 트리를 건드리는가.</b> 문자열 일치만 보면 부족하다 — Boot 는
     * {@code a} 와 {@code a[0]}, {@code a} 와 {@code a.b} 를 한 트리로 바인딩해서,
     * 상위 소스가 {@code a} 를 주면 하위 소스의 {@code a[0]} 은 <b>통째로 무시된다.</b>
     *
     * <p>{@code a[0]}·{@code a[1]}·리스트 잎 {@code a} 는 전부 같은 컬렉션이다. Boot 는 항목을 준
     * <b>첫 소스에서 멈추므로</b> 표기가 어느 쪽에 붙든 겹침이다. 그래서 양쪽을 첫 숫자 인덱스
     * 앞에서 잘라 컬렉션 루트끼리 본다 — 한쪽만 자르면 방향에 따라 판정이 갈린다.
     *
     * <p><b>Map 루트 아래는 두 가지가 다르다.</b> ⑴ 엔트리가 합쳐지므로 조상 관계는 겹침이 아니다.
     * ⑵ 이름이 하이버네이트 등에 <b>원문 그대로</b> 전달되므로 relaxed binding 이 적용되지 않는다 —
     * {@code hibernate.jdbc.time_zone} 과 {@code timezone} 은 다른 프로퍼티다.
     * 그래서 그 아래에서는 정규화하지 않고 원문 문자열로 본다.
     */
    static boolean overlaps(String rawKey, Collection<String> importedRawKeys) {
        if (isUnderMergedMapRoot(rawKey)) {
            String entry = collectionRoot(rawKey);
            return importedRawKeys.stream()
                    .filter(ConfigImportPrecedence::isUnderMergedMapRoot)
                    .map(ConfigImportPrecedence::collectionRoot)
                    .anyMatch(entry::equals);
        }

        ConfigurationPropertyName root = normalizedCollectionRoot(rawKey);
        return importedRawKeys.stream()
                .filter(imported -> !isUnderMergedMapRoot(imported))
                .map(ConfigImportPrecedence::normalizedCollectionRoot)
                .anyMatch(imported -> imported.equals(root)
                        || imported.isAncestorOf(root)
                        || root.isAncestorOf(imported));
    }

    private static boolean isUnderMergedMapRoot(String rawKey) {
        ConfigurationPropertyName name = ConfigurationPropertyName.adapt(rawKey, '.');
        return MERGED_MAP_ROOTS.stream().anyMatch(root -> root.isAncestorOf(name));
    }

    private static String collectionRoot(String rawKey) {
        Matcher matcher = NUMERIC_INDEX.matcher(rawKey);
        return matcher.find() ? rawKey.substring(0, matcher.start()) : rawKey;
    }

    private static ConfigurationPropertyName normalizedCollectionRoot(String rawKey) {
        return ConfigurationPropertyName.adapt(collectionRoot(rawKey), '.');
    }

    private Set<String> keysAfterImport() {
        Set<String> keys = new LinkedHashSet<>();
        for (int i = declaringIndex + 1; i < documents.size(); i++) {
            keys.addAll(flatten(documents.get(i)).keySet());
        }
        return keys;
    }

    /**
     * <b>첫 번째를 찾고 멈추면 안 된다.</b> 뒤 문서가 import 를 하나 더 선언하면 그 import 가
     * 그 문서를 이겨서 "뒤에 있으니 이긴다" 는 전제가 무너진다. 그런데 인덱스만 보면 여전히 통과한다.
     */
    private int indexOfImportDeclaration() {
        List<Integer> declaring = new ArrayList<>();
        for (int i = 0; i < documents.size(); i++) {
            if (flatten(documents.get(i)).containsKey(IMPORT_KEY)) {
                declaring.add(i);
            }
        }

        assertThat(declaring)
                .as(moduleResource + " 은 spring.config.import 를 정확히 한 문서에서만 선언해야 한다. "
                        + "없으면 DB 설정이 통째로 비고, 둘 이상이면 어느 문서가 이기는지 "
                        + "이 검사의 전제가 무너진다. 발견 위치=" + declaring)
                .hasSize(1);
        return declaring.get(0);
    }

    /**
     * 평탄화 결과에서 꺼낸다. 맵을 손으로 타고 내려가면 존재 판정과 값 추출이 다른 규칙을 쓰게 된다 —
     * YAML 은 {@code spring.config.import: ...} 처럼 점으로 이어 쓴 표기도 같은 프로퍼티라,
     * 그 표기에서 존재는 잡히는데 값은 못 찾는다.
     */
    private List<String> importedLocations() {
        Object value = flatten(documents.get(declaringIndex)).get(IMPORT_KEY);

        if (value == null) {
            // 빈 값이면 빈 목록이다. String.valueOf(null) 로 "null" 을 만들면 실패 메시지에
            // 그 문자열이 그대로 나와 목록을 고치라는 오해를 부른다.
            return List.of();
        }
        return value instanceof List<?> list
                ? list.stream().map(String::valueOf).toList()
                : List.of(String.valueOf(value));
    }

    private Map<String, Object> single(List<Map<String, Object>> loaded) {
        // 주석만 있는 문서는 키가 없어 우선순위에 아무 영향이 없다. 세면 거짓 실패가 난다.
        List<Map<String, Object>> withKeys = loaded.stream().filter(d -> !d.isEmpty()).toList();

        assertThat(withKeys)
                .as(importedResource + " 이 키를 가진 문서 여럿으로 갈리면 "
                        + "어느 문서가 이기는지부터 다시 따져야 한다")
                .hasSize(1);
        return withKeys.get(0);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> load(Class<?> loader, String resource) {
        LoaderOptions options = new LoaderOptions();
        // Boot 의 OriginTrackedYamlLoader 와 같은 규칙으로 읽는다. SnakeYAML 기본값은 중복 키를
        // 허용해 앞 블록을 조용히 버리는데, 그러면 이 검사가 파일의 일부만 훑으면서 초록을 낸다.
        options.setAllowDuplicateKeys(false);

        try (InputStream in = only(loader, resource).openStream()) {
            List<Map<String, Object>> loaded = new ArrayList<>();
            for (Object document : new Yaml(options).loadAll(in)) {
                // 주석만 있는 문서는 null 로 온다. 키가 없으니 충돌도 없다.
                loaded.add(document == null ? Map.of() : (Map<String, Object>) document);
            }
            return loaded;
        } catch (IOException e) {
            throw new IllegalStateException(resource + " 을 읽지 못했습니다.", e);
        } catch (YAMLException e) {
            // 한 테스트가 모듈 설정과 import 대상 두 벌을 파싱한다. SnakeYAML 의 mark 는
            // 'reader' 라 어느 파일인지 안 나와서, 중복 키를 막아 놓고도 진단이 반쪽이 된다.
            throw new IllegalStateException(resource + " 파싱에 실패했습니다. "
                    + "중복 키라면 같은 블록을 두 번 연 것이고, 뒤엣것이 앞을 통째로 덮습니다.", e);
        }
    }

    /**
     * <b>같은 이름이 둘이면 어느 모듈 것을 검사하는지 클래스패스 순서가 정한다.</b>
     * {@code application.yml.example} 은 api·batch 양쪽에 같은 이름으로 있어서, 두 모듈이 한
     * 클래스패스에 올라오는 날 남의 파일을 검사하고도 초록이 난다.
     */
    private static URL only(Class<?> loader, String resource) {
        List<URL> found;
        try {
            found = Collections.list(loader.getClassLoader().getResources(resource.substring(1)));
        } catch (IOException e) {
            throw new IllegalStateException(resource + " 을 찾지 못했습니다.", e);
        }

        assertThat(found)
                .as(resource + " 이 " + loader.getSimpleName() + " 의 클래스패스에 "
                        + found.size() + " 개 있다. 하나가 아니면 어느 모듈 것을 검사하는지 "
                        + "클래스패스 순서가 정하게 된다 — 하위 경로로 내리거나 이름을 갈라야 한다. "
                        + "0 개라면 storage 가 runtimeOnly 라 리소스가 안 왔는지부터 본다. 발견=" + found)
                .hasSize(1);
        return found.get(0);
    }

    /**
     * 잎 키를 <b>원문 그대로</b> 담는다.
     *
     * <p>정규화된 {@link ConfigurationPropertyName} 으로 담으면 Map 루트 아래에서 정보가 사라진다 —
     * {@code adapt} 가 밑줄을 지워 {@code time_zone} 과 {@code timezone} 이 같은 키가 되는데,
     * 하이버네이트에게는 다른 프로퍼티다. 정규화가 필요한 자리에서만 그때 변환한다.
     */
    static Map<String, Object> flatten(Map<String, Object> document) {
        Map<String, Object> leaves = new LinkedHashMap<>();
        collect("", document, leaves);
        return leaves;
    }

    @SuppressWarnings("unchecked")
    private static void collect(String prefix, Map<String, Object> node, Map<String, Object> leaves) {
        node.forEach((rawKey, value) -> {
            String key = prefix.isEmpty() ? String.valueOf(rawKey) : prefix + "." + rawKey;
            if (value instanceof Map<?, ?> nested) {
                if (nested.isEmpty()) {
                    // 빈 맵은 재귀해도 잎이 안 생겨 그 서브트리가 검사에서 통째로 사라진다.
                    // 경로 자체를 잎으로 남긴다.
                    leaves.put(key, Map.of());
                    return;
                }
                // 잎만 담는다. 중간 노드를 담으면 spring.datasource 하나로 전부 충돌 판정된다.
                collect(key, new LinkedHashMap<>((Map<String, Object>) nested), leaves);
            } else {
                leaves.put(key, value);
            }
        });
    }
}
