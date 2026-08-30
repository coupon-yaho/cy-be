// 마운트되는 배포 설정이 실제로 실렸는지 빈 생성 전에 확인합니다.
package com.kafkick.core.support.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import java.util.Set;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.StringUtils;

/**
 * <b>설정 마운트가 비었을 때 원인을 이름으로 말한다.</b>
 *
 * <h2>무엇이 일어나는가</h2>
 *
 * <p>{@code compose.yml} 이 호스트의 {@code ./application.yml} 을
 * {@code /app/config/application.yml} 로 bind mount 하는데, <b>그 파일이 없으면 Docker 가
 * 같은 이름의 디렉터리를 만들어 붙인다.</b> 스프링은 디렉터리를 설정 파일로 못 읽고 조용히
 * 지나가며, 그다음 <b>엉뚱한 자리에서</b> 죽는다.
 *
 * <pre>
 * Property: observation.datasource.url
 * Reason:   must not be blank
 * </pre>
 *
 * <p><b>이 메시지에는 원인이 없다.</b> 마운트가 비었다는 말도, 파일을 복사하라는 말도 없다 —
 * 읽는 사람은 관측 데이터소스 설정을 의심하며 시간을 쓴다(2026-08-30 에 그렇게 됐다).
 * 이 클래스는 <b>그 자리에 도달하기 전에</b> 진짜 원인을 이름으로 말한다.
 *
 * <h2>왜 {@code @Component} 가 아닌가</h2>
 *
 * <p><b>처음에 빈으로 만들었더니 한 번도 안 돌았다.</b> {@code @ConfigurationProperties} 의
 * 검증이 빈 생성 단계에서 먼저 터져, 이 가드가 만들어지기 전에 컨텍스트가 죽는다.
 * <b>가드는 자기 경우에 도달해야 뜻이 있다</b> — 실측으로 확인하고 여기로 옮겼다
 * ({@code EnvironmentPostProcessor} 는 빈이 하나도 만들어지기 전에 돈다).
 *
 * <p><b>등록 키가 Boot 4 에서 옮겨졌다.</b> {@code org.springframework.boot.env}
 * 였던 인터페이스가 {@code org.springframework.boot} 로 갔다 — 옛 이름으로 등록하면
 * 컴파일도 되고 기동도 되는데 <b>가드만 조용히 안 돈다.</b> 실측으로 확인했다.
 *
 * <h2>기본은 꺼짐이다</h2>
 *
 * <p>배포 경로가 둘이고 <b>설정을 마운트하는 것은 한쪽뿐</b>이다 — {@code compose.yml} 은
 * api·batch 에 마운트하지만 {@code base.yml + batch.yml} 의 batch 는 마운트 없이
 * 환경변수만으로 돈다(실측). 기본을 켬으로 두면 그 경로와 모든 테스트가 죽는다.
 * <b>마운트하는 쪽이 켠다</b> — 그 짝은 {@code DeployedConfigMountContractTest} 가 지킨다.
 */
public class DeployedConfigGuard implements EnvironmentPostProcessor, Ordered {

    /**
     * 거절을 켜는 손잡이. 기본은 꺼짐 — 마운트하는 배포 경로만 켠다.
     *
     * <p><b>이름이 환경변수와 맞아야 한다.</b> 스프링은 {@code DEPLOYED_CONFIG_REQUIRED} 를
     * {@code deployed-config.required} 로 푼다({@code _} → {@code .} 또는 {@code -}).
     * 한때 {@code deployed-config.guard.required} 로 뒀는데, 그러면 compose 가 주는
     * {@code DEPLOYED_CONFIG_REQUIRED} 가 <b>다른 키</b>가 되어 가드가 늘 꺼진 채였다 —
     * 컴파일도 되고 기동도 되는데 한 번도 안 문다. 실측으로 잡았다.
     */
    public static final String REQUIRED = "deployed-config.required";

    /**
     * <b>마운트되는 파일에만 있어야 하는 키.</b> jar 안에 같은 키를 넣으면 이 가드가 통째로
     * 무의미해진다 — 마운트가 없어도 값이 보이기 때문이다.
     * {@code DeployedConfigMountContractTest} 가 모듈 리소스에 이 키가 없는지 검사한다.
     */
    public static final String MARKER = "deployed-config.marker";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment,
            SpringApplication application) {
        if (!environment.getProperty(REQUIRED, Boolean.class, false)) {
            // 안 켠 경로에서는 조용히 지나간다. 여기서 경고를 내면 base.yml 경로가 매번
            // 울고, 그 소음이 정작 켠 경로의 실패를 묻는다.
            return;
        }
        String origin = sourceOf(environment);
        if (origin != null) {
            return;
        }
        throw new IllegalStateException(
                "배포 설정이 안 실렸습니다 — " + MARKER + " 가 비어 있습니다. "
                        + "compose 가 ./application.yml 을 마운트하는데 그 파일이 없으면 "
                        + "Docker 가 같은 이름의 디렉터리를 만들어 붙이고, 스프링은 그것을 "
                        + "오류 없이 무시합니다. 그다음 관측 데이터소스처럼 엉뚱한 자리에서 "
                        + "죽는데 그 메시지에는 원인이 없습니다. 먼저 복사하십시오: "
                        + "cp application.yml.example application.yml "
                        + "(이미 디렉터리가 생겼으면 rmdir 로 지운 뒤 복사). "
                        + "설정을 마운트하지 않는 배포 경로라면 "
                        + "DEPLOYED_CONFIG_REQUIRED=false 로 이 거절을 끄십시오. "
                        + "환경변수나 -D 로 " + MARKER + " 를 주는 것은 이 검사를 통과시키지 "
                        + "않습니다 — 이 가드가 보는 것은 값이 아니라 그 값을 준 자리입니다.");
    }

    /**
     * <b>값이 있는 것만으로 부족하다 — 어디서 왔는지 본다.</b>
     *
     * <p>{@code DEPLOYED_CONFIG_MARKER} 환경변수나 {@code -Ddeployed-config.marker} 로 같은
     * 키를 주면, <b>마운트가 비어도 가드가 통과한다.</b> 그러면 원거리 설정 오류가 그대로
     * 돌아온다 — 리뷰가 잡았다. 이 가드가 말하는 것은 <i>"값이 있다"</i> 가 아니라
     * <b>"마운트한 파일이 실렸다"</b> 이므로, 그 값을 준 자리가 <b>설정 파일</b>이어야 한다.
     *
     * <p>허용 목록이 아니라 <b>금지 목록</b>을 쓴다. 설정 파일 소스의 이름은 스프링 버전과
     * 경로에 따라 달라지는데({@code Config resource 'file [...]' via location ...}),
     * 허용 목록으로 두면 그 문자열이 바뀌는 날 <b>정상인 배포를 거절한다.</b> 반대로 막아야
     * 하는 자리는 이름이 고정돼 있다.
     *
     * @return 마커를 준 소스 이름. 파일 밖에서 왔거나 없으면 {@code null}
     */
    private String sourceOf(ConfigurableEnvironment environment) {
        for (PropertySource<?> source : environment.getPropertySources()) {
            // **맨 앞의 파사드를 건너뛴다.** 부트가 ConfigurationPropertySources.attach 로
            // 붙이는 이 소스는 자기 값이 없고 **뒤의 모든 소스를 대신 답한다** — 그래서
            // 환경변수로 준 값도 여기서 먼저 잡히고, 이름은 파일이 아니라 이것이 된다.
            // 처음에 이걸 안 걸러서 DEPLOYED_CONFIG_MARKER=faked 가 그대로 통과했다(실측).
            if (ATTACHED_FACADE.equals(source.getName())) {
                continue;
            }
            Object value = source.getProperty(MARKER);
            if (value == null || !StringUtils.hasText(String.valueOf(value))) {
                continue;
            }
            // 첫 번째로 값을 가진 소스가 실제로 이기는 소스다. 그것이 파일이 아니면
            // 뒤에 파일이 있더라도 지금 쓰이는 값은 파일 것이 아니다.
            return NON_FILE_SOURCES.contains(source.getName()) ? null : source.getName();
        }
        return null;
    }

    /**
     * 부트가 {@code ConfigurationPropertySources.attach} 로 맨 앞에 붙이는 파사드의 이름.
     *
     * <p><b>상수가 {@code private} 이라 리터럴로 쓴다.</b> 부트가 값을 바꾸면 이 가드가
     * 파사드를 못 걸러 다시 느슨해지는데, 그 상태를 {@code DeployedConfigGuardTest} 의
     * 환경변수 검사가 잡는다 — 이름을 여기 박는 대신 <b>동작으로</b> 지킨다.
     */
    private static final String ATTACHED_FACADE = "configurationProperties";

    /**
     * <b>설정 파일이 아닌 소스들.</b> 여기서 온 마커는 마운트의 증거가 아니다.
     * 이름이 스프링이 고정해 둔 상수라 버전 사이에 안 바뀐다.
     */
    private static final Set<String> NON_FILE_SOURCES = Set.of(
            StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
            StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME,
            "commandLineArgs",
            "spring.application.json");

    /**
     * <b>설정 파일을 읽은 뒤여야 한다.</b> {@code ConfigDataEnvironmentPostProcessor} 보다
     * 앞서 돌면 마커가 아직 안 실려 있어 <b>정상인 배포도 거절한다.</b> 그 뒤 아무 자리나
     * 잡으면 되므로 가장 낮은 우선순위를 쓴다.
     */
    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
