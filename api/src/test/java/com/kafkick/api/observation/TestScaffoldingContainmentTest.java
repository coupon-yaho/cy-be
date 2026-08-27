package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 테스트를 위해 만든 것이 <b>테스트 밖으로 새지 않는지</b> 본다.
 *
 * <p>같은 사고가 세 번 났고 셋 다 모양이 같다 — 테스트용 관심사가 공유 범위로 새어 나갔다.
 * <ul>
 *   <li>테스트만 쓰는 헬퍼가 main 소스셋에 남아 bootJar 에 실렸다 (MeterValueReader)
 *   <li>테스트가 JVM 전역 상태를 되돌리지 않고 나갔다 (Tomcat MBean Registry)
 *   <li>중첩 {@code @SpringBootConfiguration} 의 자동설정 제외가 컴포넌트 스캔을 타고 다른
 *       테스트 컨텍스트까지 따라갔다 — {@code ApiApplicationTests} 가
 *       "JPA metamodel must not be empty" 로 깨졌다
 * </ul>
 *
 * <p>셋 다 리뷰나 우연으로 발견됐지 테스트가 잡은 게 아니다. 여기서 잡는다.
 */
class TestScaffoldingContainmentTest {

    private static final String OBSERVATION_PACKAGE = "src/main/java/com/kafkick/api/observation";

    /**
     * main 에 있으면서 다른 main Java 파일이 직접 참조하지 않아도 되는 클래스. 이름 계약을
     * 제공하거나 Spring이 컴포넌트 스캔으로 발견하는 진입점이다.
     */
    private static final Set<String> MAIN_REFERENCE_EXEMPTIONS = Set.of(
            "MeterNames",
            "HttpMetricsFilterConfiguration");

    /**
     * 테스트 소스 검사는 OBS-3 소유 파일에만 건다. 저장소 전체로 넓히면 남의 티켓 테스트가
     * 이 클래스를 깨뜨린다 — {@code ManagementExposureTest}(OBS-20)와
     * {@code ObservationHealthContractTest} 가 실제로 걸렸고, 그 둘은 담당자에게 알렸다.
     */
    private static final List<String> OWNED_TESTS = List.of(
            "AutoInstrumentedMetersTest.java",
            "MeterValueReader.java",
            "MeterValueReaderTest.java",
            "ObservationDocReferenceTest.java",
            "ObservationMetricsContractTest.java",
            "PrometheusExposureContractTest.java");
    // 이 클래스 자신은 대상이 아니다 — 금지 패턴을 문자열 리터럴로 들고 있어 전부 오탐이 된다.

    @Test
    @DisplayName("main 의 관측 클래스는 main 이 실제로 쓰는 것만 남긴다 — 테스트 전용은 test 로")
    void mainSourceSetHoldsNoTestOnlyHelper() throws IOException {
        Path module = moduleRoot();
        List<Path> mainFiles = filesUnder(module.resolve("src/main"));

        for (Path candidate : javaFilesUnder(module.resolve(OBSERVATION_PACKAGE))) {
            String name = fileName(candidate);
            if (MAIN_REFERENCE_EXEMPTIONS.contains(name)) {
                continue;
            }
            assertThat(isReferencedByOtherMainFile(name, candidate, mainFiles))
                    .as("%s 는 main 에 있는데 main 어디에서도 쓰지 않는다. 테스트 전용이면"
                            + " src/test/java 로 옮겨라 — main 에 두면 bootJar 에 실리고, 다음"
                            + " 사람이 \"main 에 있으니 써도 되겠다\" 며 그 제약을 운영 경로로"
                            + " 끌어들인다. 이름 계약이라 참조가 없는 게 정상이면"
                            + " NAME_CONTRACTS 에 올려라", name)
                    .isTrue();
        }
    }

    @Test
    @DisplayName("전역 상태를 리플렉션으로 만지는 테스트는 @AfterAll 로 되돌린다")
    void whoeverTouchesGlobalStateRestoresIt() throws IOException {
        for (Path test : ownedTestFiles()) {
            String source = Files.readString(test);
            if (!statementLines(source).stream().anyMatch(l -> l.contains("setAccessible("))) {
                continue;
            }
            assertThat(statementLines(source))
                    .as("%s 가 리플렉션으로 전역 상태를 만지는데 @AfterAll 복원이 없다."
                            + " 켜 놓고 나가면 JVM 을 공유하는 뒤 테스트가 그 상태를 물려받는다",
                            fileName(test))
                    .anyMatch(line -> line.startsWith("@AfterAll"));
        }
    }

    /**
     * 자동설정을 제외하는 애노테이션. {@code @SpringBootApplication} 은
     * {@code @SpringBootConfiguration} 을 메타 애노테이션으로 품고 있어 같은 누출을 만든다.
     *
     * <p>이름 앞의 {@code [A-Za-z0-9_.]*} 는 정규화 이름을, 괄호 안의 {@code [^)]*} 는
     * {@code exclude} 앞에 다른 속성이 오는 경우를 받는다. {@code excludeName} 도 접두가 같아
     * 함께 걸린다. 문자열 포함 검사로는 이 셋이 전부 빠져나갔다(실측).
     */
    private static final Pattern AUTOCONFIG_EXCLUDE = Pattern.compile(
            "@[A-Za-z0-9_.]*(?:EnableAutoConfiguration|SpringBootApplication)\\([^)]*exclude");

    @Test
    @DisplayName("테스트용 설정 클래스에 자동설정 제외를 애노테이션으로 달지 않는다")
    void nestedBootConfigurationDoesNotLeakAutoConfigurationExcludes() throws IOException {
        for (Path test : ownedTestFiles()) {
            // 줄 단위로 보면 애노테이션을 줄바꿈한 순간 빠져나간다. 공백을 지우고 통째로 본다.
            String squashed = String.join("", statementLines(Files.readString(test)))
                    .replaceAll("\\s+", "");
            assertThat(AUTOCONFIG_EXCLUDE.matcher(squashed).find())
                    .as("%s 의 설정 클래스는 com.kafkick 컴포넌트 스캔에 걸린다. 여기에 자동설정"
                            + " 제외를 애노테이션으로 달면 그 제외가 다른 테스트의 컨텍스트까지"
                            + " 따라간다(실측). 제외가 필요하면 spring.autoconfigure.exclude"
                            + " 프로퍼티로 줘라", fileName(test))
                    .isFalse();
        }
    }

    /**
     * 목록이 가리키는 파일이 없으면 <b>실패</b>한다. 걸러 내면 rename 한 순간 검사가 0건이
     * 되면서 초록불이 된다 — 아무것도 안 보는 가드가 가장 나쁘다.
     */
    private List<Path> ownedTestFiles() {
        Path dir = moduleRoot().resolve("src/test/java/com/kafkick/api/observation");
        List<Path> files = OWNED_TESTS.stream().map(dir::resolve).toList();
        assertThat(files).allSatisfy(file ->
                assertThat(file).as("OWNED_TESTS 가 실재하지 않는 파일을 가리킨다").isRegularFile());
        return files;
    }

    /**
     * 주석에 적힌 문구와 실제 코드를 가른다. 금지 패턴을 <b>산문으로</b> 설명하는 주석이
     * 여럿이라, 문자열 포함 검사만 하면 전부 오탐이 된다(실제로 걸렸다).
     */
    private boolean containsStatement(String source, String token) {
        // 애노테이션은 줄 시작에 온다. 메서드 호출은 줄 중간이라 이 검사로는 못 찾는다.
        return statementLines(source).stream().anyMatch(line -> line.startsWith(token));
    }

    /** 주석·문자열이 아닌, 실제 코드로 보이는 줄만 남긴다. */
    private List<String> statementLines(String source) {
        return source.lines()
                .map(String::strip)
                .filter(line -> !line.startsWith("*") && !line.startsWith("//")
                        && !line.startsWith("/*"))
                .toList();
    }

    /**
     * <b>주석은 참조로 치지 않는다.</b> yml 주석이 클래스 이름을 설명으로 언급하는 것만으로
     * "쓰이고 있다" 가 되면 이 검사가 통째로 헛돈다 — 실제로 그렇게 한 번 빠져나갔다.
     * 자동설정 등록 파일처럼 코드가 아닌 곳의 참조는 살려야 하므로, 확장자가 아니라
     * <b>주석 여부</b>로 가른다.
     */
    private boolean isReferencedByOtherMainFile(String name, Path self, List<Path> mainFiles)
            throws IOException {
        for (Path file : mainFiles) {
            if (file.equals(self)) {
                continue;
            }
            boolean referenced = Files.readString(file).lines()
                    .map(String::strip)
                    .filter(line -> !line.startsWith("#") && !line.startsWith("*")
                            && !line.startsWith("//") && !line.startsWith("/*"))
                    .anyMatch(line -> line.contains(name));
            if (referenced) {
                return true;
            }
        }
        return false;
    }

    /** 리소스나 편집기 임시 파일이 섞이면 이름 매칭이 어긋나 오탐이 난다. */
    private List<Path> javaFilesUnder(Path root) throws IOException {
        return filesUnder(root).stream()
                .filter(path -> path.getFileName().toString().endsWith(".java"))
                .toList();
    }

    private List<Path> filesUnder(Path root) throws IOException {
        if (!Files.isDirectory(root)) {
            return List.of();
        }
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(Files::isRegularFile).toList();
        }
    }

    private String fileName(Path path) {
        String name = path.getFileName().toString();
        return name.endsWith(".java") ? name.substring(0, name.length() - ".java".length()) : name;
    }

    /** 주석·소스를 읽어야 하므로 클래스패스가 아니라 파일시스템에서 본다. */
    private Path moduleRoot() {
        Path working = Path.of("").toAbsolutePath();
        for (Path candidate : List.of(working, working.resolve("api"))) {
            if (Files.isDirectory(candidate.resolve("src/test/java"))) {
                return candidate;
            }
        }
        throw new IllegalStateException("api 모듈 루트를 찾지 못했다. 실행 디렉터리: " + working);
    }
}
