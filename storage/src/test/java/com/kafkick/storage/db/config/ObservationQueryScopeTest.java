package com.kafkick.storage.db.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>관측 풀로 나가는 질의가 회원 테이블을 건드리지 않는지</b>를 소스에서 고정한다.
 *
 * <p><b>[OBS-36] DB 계층 방어선이 생겼다.</b> 관측 계정은 더 이상 스키마 단위 {@code SELECT} 를
 * 갖지 않는다 — {@code infra/mysql/obs-grants/allowlist.txt} 의 테이블만 읽는다. 그래서
 * {@code members} 질의는 이제 DB 가 {@code MySQL 1142} 로 거부한다.
 * (부여가 {@code initdb} 가 아닌 별도 자리로 나간 이유는 두 실측이다 — initdb 시점에는
 * 테이블이 없어 {@code ERROR 1146} 이고, 스키마 GRANT 위에는 테이블 REVOKE 를 못 얹는다:
 * {@code ERROR 1147: There is no such grant defined ... on table 'members'}.)
 *
 * <p><b>그럼에도 이 테스트를 남기는 이유</b> — 계정 권한은 <b>배포된 DB 에서</b> 막고,
 * 이 테스트는 <b>CI 에서, 배포 전에</b> 막는다. 관측 질의에 {@code members} 를 적은 커밋은
 * 여기서 먼저 빨간불이 뜨므로, 1142 를 운영에서 만나기 전에 끝난다. 그리고 재부여 절차를
 * 아직 안 돌린 환경(OBS-36 이전 볼륨)에서는 계정 권한 쪽 방어선이 아직 없다.
 *
 * <p><b>이것이 무엇을 못 막는지 분명히 적는다.</b> 그물코가 넷 있다.
 *
 * <ol>
 *   <li><b>DB 에 직접 붙는 경로를 못 본다.</b> 저장소 밖 도구가 그 계정으로 조회하면 이
 *       테스트는 아무것도 모른다. 그쪽은 이제 계정 권한이 막는다(OBS-36) — 단
 *       {@code obs-grants} 재부여를 돌린 환경에서만 그렇다</li>
 *   <li><b>{@code src/main/java} 만 걷는다.</b> 질의가 {@code .sql}·{@code .yml} 로 나가면
 *       안 본다. 지금 그런 파일은 없다(확인함) — 생기는 날 이 테스트는 조용히 통과한다</li>
 *   <li><b>뷰·별칭 경유를 못 본다.</b> {@code V8__latest_stats_run_view.sql} 처럼 뷰가 실재하므로,
 *       뷰가 {@code members} 를 감싸면 질의문에는 그 이름이 안 나온다</li>
 *   <li><b>표지가 {@code @Qualifier("obs")} 문자열이다.</b> 그 한정자 없이 빈 이름으로만
 *       주입받는 파일이 생기면 대상에서 빠진다. 지금은 전부 한정자를 달고 있다</li>
 * </ol>
 *
 * <p>즉 이것은 <b>개발 시점 회귀를 잡는 그물</b>이고 보안 경계가 아니다. 그렇게 읽히지 않도록
 * 여기 적어 둔다.
 *
 * <p>덧붙여 {@code members} 의 개인정보 컬럼은 이미 암호화돼 있다
 * ({@code name_enc}·{@code email_enc}·{@code phone_enc} 가 {@code varbinary}, AES-256-GCM).
 * 읽어도 평문은 안 나온다. 남는 노출은 HMAC 블라인드 인덱스와 등급·가입 시각이다.
 */
class ObservationQueryScopeTest {

    /**
     * 걷는 모듈. <b>{@code settings.gradle} 의 목록과 같아야 한다</b> — 예전에 넷만 적어 두어
     * {@code infra:mq}·{@code infra:redis} 가 빠져 있었다. 그 모듈이 관측 풀을 쓰기 시작하면
     * 이 테스트는 <b>조용히 통과한다.</b> 아래 계약 테스트가 그 어긋남을 잡는다.
     */
    private static final List<String> MODULES =
            List.of("api", "batch", "core", "storage", "infra/mq", "infra/redis");

    /** 관측 풀을 무는 표지. 이 문자열이 있는 파일이 검사 대상이다. */
    private static final String OBSERVATION_QUALIFIER = "@Qualifier(\"obs\")";

    /** 회원 테이블. 단어 경계로 본다 — {@code membership_grade} 같은 컬럼명에 걸리면 안 된다. */
    private static final Pattern MEMBERS = Pattern.compile("\\bmembers\\b", Pattern.CASE_INSENSITIVE);

    /** 자바 문자열 리터럴. 질의문만 보려는 것이라 주석·식별자는 대상이 아니다. */
    private static final Pattern STRING_LITERAL = Pattern.compile("\"([^\"\\\\]|\\\\.)*\"");

    @Test
    @DisplayName("관측 한정자를 쓰는 코드의 질의문에 members 가 없다")
    void observationQueriesNeverTouchMembers() {
        List<Path> consumers = observationConsumers();

        assertThat(consumers)
                .as("검사 대상이 하나도 없으면 이 테스트는 아무것도 검증하지 않는다")
                .isNotEmpty();

        List<String> offenders = new ArrayList<>();
        for (Path file : consumers) {
            for (String literal : stringLiteralsOf(file)) {
                if (MEMBERS.matcher(literal).find()) {
                    offenders.add(file.getFileName() + " → " + literal.trim());
                }
            }
        }

        assertThat(offenders)
                .as("관측 풀은 회원 테이블을 읽을 이유가 없다. 계정 권한이 그것을 막지 못하므로 "
                        + "(클래스 javadoc 참조) 여기가 유일한 그물이다")
                .isEmpty();
    }

    @Test
    @DisplayName("걷는 모듈 목록이 settings.gradle 과 같다")
    void moduleListMatchesSettingsGradle() {
        // 계약이 두 파일에 걸친다. 각각을 따로 보는 테스트로는 "빠진 모듈" 을 못 잡는다 —
        // 빠지면 대상이 줄어들 뿐 실패하지 않기 때문이다.
        List<String> declared = Pattern.compile("^include\\s+[\"']([^\"']+)[\"']",
                        Pattern.MULTILINE)
                .matcher(read(repoRoot().resolve("settings.gradle")))
                .results()
                .map(match -> match.group(1).replace(':', '/'))
                .toList();

        assertThat(declared).as("settings.gradle 에서 모듈을 하나도 못 읽었다").isNotEmpty();
        assertThat(MODULES)
                .as("모듈이 늘면 여기도 늘려야 한다. 안 그러면 그 모듈의 관측 질의를 아무도 안 본다")
                .containsExactlyInAnyOrderElementsOf(declared);
    }

    private static List<Path> observationConsumers() {
        List<Path> found = new ArrayList<>();
        for (String module : MODULES) {
            Path main = repoRoot().resolve(module).resolve("src/main/java");
            if (!Files.isDirectory(main)) {
                continue;
            }
            try (Stream<Path> paths = Files.walk(main)) {
                paths.filter(p -> p.toString().endsWith(".java"))
                        .filter(p -> read(p).contains(OBSERVATION_QUALIFIER))
                        .forEach(found::add);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return found;
    }

    private static List<String> stringLiteralsOf(Path file) {
        List<String> literals = new ArrayList<>();
        Matcher matcher = STRING_LITERAL.matcher(read(file));
        while (matcher.find()) {
            literals.add(matcher.group());
        }
        return literals;
    }

    private static String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 작업 디렉터리가 모듈마다 달라 위로 올라가며 {@code settings.gradle} 로 찾는다.
     * 상대 경로를 박아 두면 다른 모듈에서 돌릴 때 조용히 대상 0개가 되어 통과한다.
     */
    private static Path repoRoot() {
        Path candidate = Path.of("").toAbsolutePath();
        while (candidate != null && !Files.exists(candidate.resolve("settings.gradle"))) {
            candidate = candidate.getParent();
        }
        if (candidate == null) {
            throw new IllegalStateException("settings.gradle 을 못 찾았다. 저장소 루트를 알 수 없다");
        }
        return candidate;
    }
}
