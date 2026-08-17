package com.kafkick.storage.db.config;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

/**
 * 낡은 {@code management.yml} 로 뜨는 것을 기동 시점에 막는다.
 *
 * <p>이 계약은 코드와 설정 파일에 걸쳐 있다 — {@link ObservationHealthConfig#OBSERVATION_DOWN} 과
 * {@code management.endpoint.health.status.order}. 그런데 {@code management.yml} 은 커밋하지 않으므로,
 * 이 PR 이전의 파일을 그대로 가진 사람은 <b>순서 목록 없이</b> 뜬다. 그러면 관측 풀 장애가 그대로
 * {@code DOWN} 으로 합산되어 발급 API 가 멀쩡한데 인스턴스가 로드밸런서에서 빠진다 — 에러도 경고도
 * 없이 그렇게 된다. 이 티켓이 막으려던 것 그 자체다.
 *
 * <p><b>왜 storage 인가.</b> 검사 대상은 api 가 소유한 설정이지만, api 의 storage 의존은
 * {@code runtimeOnly} 라 api 본 코드가 이 상수를 참조할 수 없다. api 에 두면 문자열 사본이 세 번째로
 * 생겨 지금 고친 문제가 그대로 재발한다. 상수를 아는 쪽이 검사도 한다.
 *
 * <p><b>검사 범위 — 알면서 남긴 구멍.</b> {@code management.endpoint.health} 아래에 설정이
 * <b>하나라도</b> 있을 때만 검사한다. 관리 설정을 아예 안 하는 JVM(설정 파일 없이 뜨는 테스트
 * 컨텍스트가 그렇다)까지 막으면, 이 검사가 관측과 무관한 것을 죽인다. 낡은 파일은 최소한
 * {@code show-details} 같은 키를 갖고 있으므로 이 조건으로 걸린다.
 */
class ObservationHealthStatusOrderCheck implements InitializingBean {

    static final String HEALTH_PREFIX = "management.endpoint.health";

    static final String STATUS_ORDER = HEALTH_PREFIX + ".status.order";

    private static final String GROUP = HEALTH_PREFIX + ".group." + ObservationHealthConfig.HEALTH_GROUP;

    static final String GROUP_INCLUDE = GROUP + ".include";

    static final String GROUP_MAPPING = GROUP + ".status.http-mapping";

    /** 200 을 돌려주는 그룹은 경보를 걸 수 없다. 실패로 보여야 한다. */
    private static final int FAILURE_CODE_FLOOR = 400;

    private final Environment environment;

    ObservationHealthStatusOrderCheck(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void afterPropertiesSet() {
        Binder binder = Binder.get(environment);
        Map<String, Object> health = binder.bind(HEALTH_PREFIX, Bindable.mapOf(String.class, Object.class))
            .orElseGet(Map::of);
        if (health.isEmpty()) {
            return;
        }

        String code = ObservationHealthConfig.OBSERVATION_DOWN.getCode();
        requireSeverityOrder(binder, code);
        requireGroupMembership(binder);
        requireGroupReportsFailure(binder, code);
    }

    /** 이게 없으면 관측 풀 장애가 합산 상태를 끌어내려 인스턴스가 로드밸런서에서 빠진다. */
    private static void requireSeverityOrder(Binder binder, String code) {
        List<String> order = binder.bind(STATUS_ORDER, Bindable.listOf(String.class)).orElseGet(List::of);
        if (!order.contains(code)) {
            throw stale(STATUS_ORDER + " 에 " + code + " 가 없다."
                + " 관측 풀 장애가 그대로 합산돼 인스턴스가 로드밸런서에서 빠진다. (현재 값: " + order + ")");
        }
    }

    /** 이게 없으면 관측 풀 장애를 볼 창구가 사라진다 — 합산은 UP 이므로 어디에도 안 드러난다. */
    private static void requireGroupMembership(Binder binder) {
        List<String> include = binder.bind(GROUP_INCLUDE, Bindable.listOf(String.class)).orElseGet(List::of);
        if (!include.contains(ObservationHealthConfig.CONTRIBUTOR_ID)) {
            throw stale(GROUP_INCLUDE + " 가 " + ObservationHealthConfig.CONTRIBUTOR_ID + " 를 포함하지 않는다."
                + " 관측 풀 장애를 볼 창구가 없다. (현재 값: " + include + ")");
        }
    }

    /**
     * 창구가 있어도 늘 200 을 돌려주면 경보를 걸 수 없다. 기본 매핑에는 이 상태가 없어서
     * <b>적지 않으면 200 이 된다</b> — 빠뜨리기 가장 쉬운 자리다.
     */
    private static void requireGroupReportsFailure(Binder binder, String code) {
        // 키를 프로퍼티 이름으로 직접 붙이면 안 된다 — OBSERVATION_DOWN 은 대문자·밑줄이라
        // ConfigurationPropertyName 규칙에 맞지 않아 InvalidConfigurationPropertyNameException 이 난다.
        // 맵으로 바인딩하면 상태 코드가 이름이 아니라 값이 되어 원문 그대로 들어온다.
        Map<String, Integer> mapping = binder.bind(GROUP_MAPPING, Bindable.mapOf(String.class, Integer.class))
            .orElseGet(Map::of);
        Integer status = mapping.get(code);
        if (status == null || status < FAILURE_CODE_FLOOR) {
            throw stale(GROUP_MAPPING + " 의 " + code + " 가 실패 코드가 아니다(현재 값: " + status + ")."
                + " 관측 풀이 죽어도 그룹이 200 을 돌려줘 경보가 울리지 않는다.");
        }
    }

    private static IllegalStateException stale(String detail) {
        return new IllegalStateException(
            detail + " management.yml 을 management.yml.example 로 다시 복사한다.");
    }
}
