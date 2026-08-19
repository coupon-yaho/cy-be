package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * OBS-3 이 소유한 파일의 주석이 <b>존재하지 않는 테스트 클래스</b>를 가리키지 않는지 본다.
 *
 * <p>주석에 적힌 "이건 XxxTest 가 지킨다" 는 컴파일러가 검증하지 않는다. 테스트 클래스를
 * rename 하면 주석이 조용히 거짓이 되고, 그 상태에서 전체 테스트와 javadoc 이 전부 통과한다
 * (실측).
 *
 * <p><b>{@code @link} 로는 못 막는다.</b> main javadoc 이 test 클래스를 {@code @link} 하면
 * 컴파일은 통과하고 {@code javadoc} 태스크만 "reference not found" 로 깨진다 — 부패는 못 막고
 * 빌드만 잃는다(실측).
 *
 * <p><b>검사 범위는 OBS-3 소유 파일뿐이다</b> — main 설정·상수({@link #OWNED_FILES})와 이
 * 티켓이 만든 테스트 소스({@link #OWNED_JAVA})를 함께 본다. 검사마다 대상이 다르다: 클래스명
 * 인용은 양쪽 전부, javadoc 위치와 TODO 는 자바 소스만, 결정 번호는
 * {@link #FILES_CITING_DECISIONS}. api 전체로 넓히면 남의 티켓 주석이 이 테스트를 깨뜨린다.
 *
 * <p>결정 번호 인용도 함께 본다. 근거 문서 없이 번호만 적으면 읽는 사람이 찾아갈 곳이 없어
 * <b>저장소 안에 그 번호를 정의한 문서가 있을 때만</b> 허용한다.
 *
 * <p>주석이 <b>제 선언에 붙어 있는지</b>와 TODO 에 <b>담당자가 적혀 있는지</b>도 본다.
 * 컴파일러는 주석을 검증하지 않으므로, 선언에서 떨어진 javadoc 은 조용히 버려지고 그 선언은
 * 무문서가 된다. 이 저장소에서 같은 결함이 세 번 났다.
 *
 * <p><b>잡지 못하는 것 — 티켓 번호.</b> 주석 속 {@code OBS-5} 가 틀려도 여기서는 아무 일도
 * 일어나지 않는다. 티켓은 저장소 밖(Jira)에 있어 대조할 원본이 없다.
 */
class ObservationDocReferenceTest {

    /**
     * {@code Test} 로 끝나지만 <b>우리 테스트가 아닌</b> 프레임워크 식별자. 애노테이션 형태는
     * {@code @} 로 걸러지지만 {@code SpringBootTest.WebEnvironment} 처럼 타입으로 쓰이는 것도
     * 있어 이름으로 한 번 더 거른다.
     */
    private static final Set<String> FRAMEWORK_NAMES = Set.of(
            "Test", "SpringBootTest", "DataJpaTest", "TestConfiguration", "WebMvcTest");

    /** 대문자로 시작하고 {@code Test} 로 끝나는 식별자만 본다 — 산문의 "Test" 는 거른다. */
    private static final Pattern TEST_CLASS_NAME = Pattern.compile("\\b([A-Z][A-Za-z0-9_]*Test)\\b");

    /**
     * TODO 담당자. 한 글자짜리 역할 라벨({@code @A})이 형식만 만족시키고 통과하던 것을 막는다 —
     * 실제 계정 핸들이어야 알림이 가고 추적이 된다.
     */
    private static final Pattern TODO_OWNER = Pattern.compile("@[A-Za-z0-9_-]{2,}");

    /** 결정 번호 인용. 근거 문서가 저장소에 있어야만 쓸 수 있다. */
    private static final Pattern DECISION_ID = Pattern.compile("\\b((?:DEC|OD)-\\d+)\\b");

    /** OBS-3 소유 파일. 자동 수집하면 남의 파일까지 끌어와 남의 커밋에서 깨진다. */
    private static final List<String> OWNED_FILES = List.of(
            "src/main/java/com/kafkick/api/observation/MeterNames.java",
            "src/main/resources/observation.yml.example",
            "src/main/resources/application.yml.example");

    /**
     * 결정 번호 인용은 테스트 소스까지 본다. 반면 위 {@link #OWNED_FILES} 의 테스트 클래스명
     * 검사는 main 파일에만 건다 — 테스트 소스에는 {@code @SpringBootTest} 처럼 {@code Test} 로
     * 끝나는 <b>프레임워크 식별자</b>가 널려 있어 전부 오탐이 된다(실제로 걸렸다).
     */
    private static final List<String> FILES_CITING_DECISIONS = Stream.concat(
            OWNED_FILES.stream(),
            Stream.of("src/test/java/com/kafkick/api/observation/MeterValueReader.java",
                    "src/test/java/com/kafkick/api/observation/PrometheusExposureContractTest.java",
                    "src/test/java/com/kafkick/api/observation/AutoInstrumentedMetersTest.java",
                    "src/test/java/com/kafkick/api/observation/MeterValueReaderTest.java",
                    "src/test/java/com/kafkick/api/observation/ObservationMetricsContractTest.java"))
            .toList();

    /** javadoc 검사 대상. 주석이 붙을 선언이 있는 자바 소스만 본다. */
    private static final List<String> OWNED_JAVA = List.of(
            "src/main/java/com/kafkick/api/observation/MeterNames.java",
            "src/test/java/com/kafkick/api/observation/MeterValueReader.java",
            "src/test/java/com/kafkick/api/observation/AutoInstrumentedMetersTest.java",
            "src/test/java/com/kafkick/api/observation/MeterValueReaderTest.java",
            "src/test/java/com/kafkick/api/observation/ObservationMetricsContractTest.java",
            "src/test/java/com/kafkick/api/observation/PrometheusExposureContractTest.java",
            "src/test/java/com/kafkick/api/observation/TestScaffoldingContainmentTest.java");


    @Test
    @DisplayName("OBS-3 소유 파일의 주석이 가리키는 테스트 클래스가 전부 실재한다")
    void everyReferencedTestClassExists() throws IOException {
        Path module = moduleRoot();
        Set<String> existing = existingTestClassNames(module);

        for (String owned : allOwnedFiles()) {
            Path file = module.resolve(owned);
            assertThat(file).as("OBS-3 소유 파일이 목록과 어긋난다").exists();

            for (String cited : citedTestClassNames(Files.readString(file))) {
                assertThat(existing)
                        .as("%s 의 주석이 없는 테스트 %s 를 가리킨다", owned, cited)
                        .contains(cited);
            }
        }
    }

    @Test
    @DisplayName("javadoc 이 제 선언에 붙어 있다 — 연달아 두 개면 앞의 것이 버려진다")
    void noJavadocIsOrphaned() throws IOException {
        Path module = moduleRoot();
        for (String owned : OWNED_JAVA) {
            List<String> lines = Files.readAllLines(module.resolve(owned));
            assertThat(orphanedJavadocLines(lines))
                    .as("%s — javadoc 블록 바로 뒤에 또 javadoc 이 온다. 자바는 선언에 인접한"
                            + " 하나만 인정하므로 앞의 주석은 버려지고, 그게 설명하던 선언은"
                            + " 무문서가 된다. 각 주석을 제 선언 위로 옮겨라", owned)
                    .isEmpty();
        }
    }

    @Test
    @DisplayName("TODO 에는 담당자가 붙어 있다 — 이름 없는 TODO 는 아무도 안 가져간다")
    void everyTodoNamesAnOwner() throws IOException {
        Path module = moduleRoot();
        for (String owned : OWNED_JAVA) {
            for (String line : Files.readAllLines(module.resolve(owned))) {
                if (!line.contains("TODO")) {
                    continue;
                }
                assertThat(TODO_OWNER.matcher(line).find())
                        .as("%s 의 TODO 에 추적 가능한 담당자가 없다. TODO(티켓, @핸들) 형식으로"
                                + " 적어라 — 역할 라벨(@A)이나 빈 @ 는 알림도 안 가고 아무도"
                                + " 가져가지 않는다", owned)
                        .isTrue();
            }
        }
    }

    /** 여는 javadoc 이 닫힌 직후 다시 javadoc 이 오는 지점의 줄 번호(1-based). */
    private List<Integer> orphanedJavadocLines(List<String> lines) {
        List<Integer> orphans = new java.util.ArrayList<>();
        boolean inJavadoc = false;
        boolean justClosed = false;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).strip();
            if (justClosed && !line.isEmpty()) {
                if (line.startsWith("/**")) {
                    orphans.add(i + 1);
                }
                justClosed = false;
            }
            if (!inJavadoc && line.startsWith("/**")) {
                inJavadoc = !line.endsWith("*/") || line.equals("/**");
                justClosed = !inJavadoc;
            } else if (inJavadoc && line.endsWith("*/")) {
                inJavadoc = false;
                justClosed = true;
            }
        }
        return orphans;
    }

    @Test
    @DisplayName("근거 문서 없는 결정 번호를 인용하지 않는다 — 읽는 사람이 찾아갈 곳이 있어야 한다")
    void everyCitedDecisionIdIsBackedByADocument() throws IOException {
        Path module = moduleRoot();
        Set<String> defined = decisionIdsDefinedInDocs(module.getParent().resolve("docs"));

        for (String owned : FILES_CITING_DECISIONS) {
            Matcher matcher = DECISION_ID.matcher(Files.readString(module.resolve(owned)));
            while (matcher.find()) {
                assertThat(defined)
                        .as("%s 가 %s 를 인용하는데 docs/ 에 그 번호를 정의한 문서가 없다."
                                + " 문서를 커밋하거나 주석에서 번호를 빼라", owned, matcher.group(1))
                        .contains(matcher.group(1));
            }
        }
    }

    /** {@code ## DEC-02 ...} 처럼 제목으로 번호를 정의한 것만 정의로 인정한다. */
    private Set<String> decisionIdsDefinedInDocs(Path docs) throws IOException {
        if (!Files.isDirectory(docs)) {
            return Set.of();
        }
        Pattern heading = Pattern.compile("^#{1,6}\\s*((?:DEC|OD)-\\d+)", Pattern.MULTILINE);
        Set<String> defined = new java.util.HashSet<>();
        try (Stream<Path> files = Files.walk(docs)) {
            for (Path file : files.filter(f -> f.toString().endsWith(".md")).toList()) {
                Matcher matcher = heading.matcher(Files.readString(file));
                while (matcher.find()) {
                    defined.add(matcher.group(1));
                }
            }
        }
        return defined;
    }

    /**
     * 산문이 <b>인용한</b> 테스트 클래스 이름만 고른다. 애노테이션({@code @SpringBootTest})과
     * import 의 마지막 마디({@code ...api.Test})는 프레임워크 식별자라 검사 대상이 아니다 —
     * 걸러 내지 않으면 테스트 소스를 훑는 순간 전부 오탐이 된다.
     */
    private Set<String> citedTestClassNames(String source) {
        Set<String> cited = new java.util.LinkedHashSet<>();
        Matcher matcher = TEST_CLASS_NAME.matcher(source);
        while (matcher.find()) {
            int before = matcher.start() - 1;
            char preceding = before >= 0 ? source.charAt(before) : ' ';
            if (preceding != '@' && preceding != '.'
                    && !FRAMEWORK_NAMES.contains(matcher.group(1))) {
                cited.add(matcher.group(1));
            }
        }
        return cited;
    }

    private List<String> allOwnedFiles() {
        return Stream.concat(OWNED_FILES.stream(), OWNED_JAVA.stream()).distinct().toList();
    }

    private Set<String> existingTestClassNames(Path module) throws IOException {
        try (Stream<Path> sources = Files.walk(module.resolve("src/test/java"))) {
            return sources
                    .map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(".java"))
                    .map(name -> name.substring(0, name.length() - ".java".length()))
                    .collect(Collectors.toSet());
        }
    }

    /**
     * 주석은 컴파일되면 사라지므로 클래스패스가 아니라 파일시스템에서 읽는다. Gradle(모듈
     * 디렉터리)과 IDE(저장소 루트) 양쪽을 받아 주고, 어느 쪽도 아니면 skip 이 아니라 실패한다 —
     * skip 하면 아무것도 검사하지 않으면서 초록불이 된다.
     */
    private Path moduleRoot() {
        Path working = Path.of("").toAbsolutePath();
        for (Path candidate : List.of(working, working.resolve("api"))) {
            if (Files.isDirectory(candidate.resolve("src/test/java"))) {
                return candidate;
            }
        }
        throw new IllegalStateException(
                "api 모듈 루트를 찾지 못했다. 실행 디렉터리: " + working);
    }
}
