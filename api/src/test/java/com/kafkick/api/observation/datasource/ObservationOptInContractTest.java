package com.kafkick.api.observation.datasource;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

/**
 * 관측 옵트인 스위치는 <b>두 파일에 걸친 계약</b>이라 여기서 잇는다.
 *
 * <ul>
 *   <li>{@code application.yml} — {@code observation.datasource.enabled} 가 관측 빈을 만들지 정한다
 *   <li>{@code management.yml} — health {@code group.obs} 가 그 빈(obsDb 기여자)을 이름으로 지목한다
 * </ul>
 *
 * <p>둘은 같이 켜지거나 같이 꺼져야 한다. 어긋나면 이렇게 된다.
 *
 * <table border="1">
 *   <tr><th>스위치</th><th>그룹</th><th>결과</th></tr>
 *   <tr><td>끔</td><td>있음</td><td>지목한 기여자가 없어 <b>기동 실패</b></td></tr>
 *   <tr><td>켬</td><td>없음</td><td>관측 풀 장애를 <b>아무도 모른다</b> — 조용하다</td></tr>
 * </table>
 *
 * <p>앞의 경우는 시끄러워서 알아서 드러나지만, 뒤의 경우가 이 티켓이 막으려는 실패다. 각 파일을
 * 따로 검증하는 테스트로는 둘의 어긋남을 잡을 수 없어 한 자리에서 본다.
 *
 * <p>실제 {@code application.yml}·{@code management.yml} 은 커밋하지 않으므로 템플릿을 읽는다.
 */
class ObservationOptInContractTest {

    private static final String SWITCH = "observation.datasource.enabled";

    private static final String GROUP_INCLUDE = "management.endpoint.health.group.obs.include";

    @Test
    @DisplayName("api 는 관측을 켠다 — 대시보드 집계를 운영 풀에서 떼어 내는 것이 이 모듈의 몫이다")
    void apiOptsIn() {
        assertThat(parse("application.yml.example").get(SWITCH))
            .as("이 값이 없으면 관측 빈이 통째로 안 만들어진다").hasToString("true");
    }

    /**
     * 스위치는 모듈마다 값이 달라야 하는 키다(api 는 켜고 batch 는 끈다). 공유 파일인 storage.yml 에
     * 있으면 모듈 선언을 조용히 이기므로, 거기 없다는 것까지 확인한다.
     */
    @Test
    @DisplayName("스위치는 공유 파일이 아니라 모듈이 소유한다")
    void theSwitchIsOwnedByTheModule() {
        assertThat(parse("storage.yml.example").get(SWITCH))
            .as("공유 파일이 선언하면 모듈이 끄지 못한다").isNull();
    }

    @Test
    @DisplayName("켰으면 obs 그룹도 있어야 한다 — 없으면 관측 풀 장애를 아무도 모른다")
    void turningItOnRequiresTheHealthGroup() {
        boolean enabled = "true".equals(String.valueOf(parse("application.yml.example").get(SWITCH)));
        Object group = parse("management.yml.example").get(GROUP_INCLUDE);

        assertThat(enabled).isTrue();
        assertThat(group).as("스위치를 켰으면 obs 그룹이 그 기여자를 지목해야 한다").hasToString("obsDb");
    }

    private static Properties parse(String resource) {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource(resource));
        return yaml.getObject();
    }
}
