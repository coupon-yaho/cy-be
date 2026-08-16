// 빌드가 만든 .example 사본이 제대로 만들어졌고 전부 해석되는지 확인합니다.
package com.kafkick.storage.db.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;

/**
 * <b>복사본을 만드는 자리가 루트 한 벌이니 검증도 한 벌이어야 합니다.</b>
 * 모듈마다 복사하면 한쪽만 고쳐지고 다른 쪽은 낡은 규칙으로 통과합니다 —
 * {@link ConfigImportPrecedence} 를 공유한 이유와 같습니다.
 */
public final class ResolvedConfigChecks {

    private static final String RESOLVED_APP = "/resolved/application.yml";
    private static final String RESOLVED_STORAGE = "resolved/storage.yml";

    private ResolvedConfigChecks() {
    }

    /**
     * 산출물에 import 치환이 실제로 적용됐는가.
     *
     * <p>Gradle 의 up-to-date 판정이 절대 건너뛸 수 없는 자리다. 치환이 빗나가면
     * {@code classpath:storage.yml} 로 <b>개발자가 로컬에 만든</b> storage.yml 을 읽는데,
     * 그 파일도 {@code .example} 사본이라 값 단언이 전부 통과한다 —
     * 저장소가 관리하지 않는 파일을 보면서 초록이 된다.
     */
    public static void assertCopyPointsAtResolvedStorage(Class<?> loader) throws IOException {
        try (InputStream in = loader.getResourceAsStream(RESOLVED_APP)) {
            assertThat(in)
                    .as("복사본이 아예 없다. 루트 build.gradle 의 processTestResources 를 확인해라")
                    .isNotNull();

            String yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(yaml)
                    .as("치환이 죽었거나 산출물이 낡았다")
                    .contains("classpath:/resolved/storage.yml")
                    .doesNotContain("import: classpath:storage.yml");
        }
    }

    /** 실제로 읽은 것이 저장소가 관리하는 사본인가. 값만 보면 로컬 파일을 읽어도 통과한다. */
    public static void assertReadsResolvedStorage(ConfigurableEnvironment environment) {
        List<String> names = new ArrayList<>();
        environment.getPropertySources().forEach(source -> names.add(source.getName()));

        assertThat(names)
                .as("저장소가 관리하는 복사본을 읽었는지부터 본다")
                .anySatisfy(name -> assertThat(name).contains(RESOLVED_STORAGE));
    }

    /**
     * 사본의 <b>모든</b> 키가 해석되는가 — 기본값 없는 플레이스홀더를 잡는다.
     *
     * <p>몇 개를 골라 읽는 것으로는 부족하다. {@code Environment.getProperty} 는 호출된 키만
     * 치환하므로, 아무도 안 읽는 키에 {@code ${OBS_DB_URL}} 같은 것이 들어와도 조용하다.
     * 그러면 운영 기동에서 {@code Could not resolve placeholder} 로 죽는데,
     * compose 가 없어 배포 전에 띄워 보는 경로도 없다.
     *
     * <p><b>소스의 원문을 직접 해석한다.</b> 키 이름만 꺼내 {@code getProperty} 를 부르면
     * 우선순위가 가장 높은 값만 돌아와서, 같은 이름을 가진 <b>진 문서의 원문은 한 번도 안 본다</b> —
     * {@code maximum-pool-size} 처럼 두 파일에 다 있는 키가 실제로 그렇다.
     */
    public static void assertEveryPlaceholderResolves(ConfigurableEnvironment environment) {
        int scanned = 0;

        for (PropertySource<?> source : environment.getPropertySources()) {
            // 사본에서 온 소스만 본다. 커맨드라인 인자까지 돌 이유가 없다.
            if (!(source instanceof EnumerablePropertySource<?> enumerable)
                    || !source.getName().contains("resolved/")) {
                continue;
            }
            scanned++;

            for (String name : enumerable.getPropertyNames()) {
                if (!(enumerable.getProperty(name) instanceof String text) || !text.contains("${")) {
                    continue;
                }
                assertThatCode(() -> environment.resolveRequiredPlaceholders(text))
                        .as(source.getName() + " 의 " + name + " = " + text + " 에 기본값이 없다")
                        .doesNotThrowAnyException();
            }
        }

        assertThat(scanned)
                .as("resolved/ 소스를 하나도 못 찾았다 — 루트 build.gradle 의 into 'resolved' 와 "
                        + "이 클래스의 이름 규칙이 어긋났다. 공허하게 통과시키지 않는다")
                .isNotZero();
    }
}
