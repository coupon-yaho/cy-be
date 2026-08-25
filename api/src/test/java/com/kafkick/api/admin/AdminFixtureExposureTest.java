package com.kafkick.api.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.kafkick.api.admin.support.config.AdminFixtureConfig;
import com.kafkick.core.admin.couponmetrics.AdminCouponMetricsService;
import com.kafkick.core.admin.couponmetrics.mock.AdminCouponMetricsMockDataFactory;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryService;
import com.kafkick.core.admin.inquiry.mock.AdminIssuanceInquiryMockDataFactory;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryService;
import com.kafkick.core.admin.issuancehistory.mock.AdminIssuanceHistoryMockDataFactory;
import com.kafkick.core.admin.overview.AdminOverviewService;
import com.kafkick.core.admin.overview.mock.AdminOverviewMockDataFactory;
import com.kafkick.core.admin.analytics.AdminAnalyticsService;

/**
 * <b>운영 기본값에서 관리자 화면 fixture 가 응답에 실리지 않는지</b>를 고정한다.
 *
 * <p>[OBS-36] 네 mock factory 는 조건 없는 {@code @Component} 였다. 그래서 <b>운영 200 이
 * fixture 를 반환했다</b> — 관측 계정이 members 를 읽던 것과 같은 축의 문제다: 운영에 남으면
 * 안 되는 것이 아무 증상 없이 남아 있었다. 지금은 {@code admin.mock.enabled} 가 기본 꺼짐이다.
 *
 * <p><b>끈 상태가 PENDING 이 아니라 기동 실패인 이유.</b> 네 화면의 응답 계약에는 "아직
 * 집계되지 않았음" 을 담을 자리가 없다 — 예컨대 {@code AdminIssuanceInquiryResult} 는
 * {@code items}·{@code nextBefore}·{@code hasOlder} 뿐이라, 빈 결과를 내면 <b>"조회했는데
 * 없음" 과 구분되지 않는다.</b> 그 구분을 만드는 것은 응답 계약 변경이고 A-06·A-07·A-08·A-10
 * 의 몫이라 여기서 발명하지 않았다. {@code analytics} 만 그 자리가 이미 있어서(PendingSource)
 * 혼자 다르게 동작한다 — 아래 인벤토리가 그 비대칭을 명시한다.
 */
class AdminFixtureExposureTest {

    /**
     * <b>아직 fixture 를 쓰는 화면과 그 인계 티켓.</b> 이 목록이 이 티켓이 남긴 빚의 전부다.
     *
     * <p>A 티켓이 실제 Repository 를 붙이면 <b>여기서 한 줄을 지우는 것이 강제된다</b> —
     * 아래 단언이 생성자 시그니처를 직접 읽기 때문이다. 그래서 빚이 조용히 남지도, 조용히
     * 늘지도 않는다.
     */
    private static final List<FixtureBackedScreen> FIXTURE_BACKED_SCREENS = List.of(
            new FixtureBackedScreen("운영현황", "A-06",
                    AdminOverviewService.class, AdminOverviewMockDataFactory.class),
            new FixtureBackedScreen("회원 발급 문의", "A-07",
                    AdminIssuanceInquiryService.class, AdminIssuanceInquiryMockDataFactory.class),
            new FixtureBackedScreen("발급 이력", "A-08",
                    AdminIssuanceHistoryService.class, AdminIssuanceHistoryMockDataFactory.class),
            new FixtureBackedScreen("상세 지표", "A-10",
                    AdminCouponMetricsService.class, AdminCouponMetricsMockDataFactory.class));

    /**
     * 네 factory 에 걸린 스위치. <b>문자열을 옮겨 적지 않고 배선이 쓰는 상수를 그대로 참조한다</b> —
     * 옮겨 적으면 배선이 키를 바꿔도 이 테스트는 옛 이름으로 계속 초록불이다.
     */
    private static final String FIXTURE_SWITCH = AdminFixtureConfig.FIXTURE_SWITCH;

    private record FixtureBackedScreen(
            String screen, String handoverTicket, Class<?> service, Class<?> mockFactory) {
    }

    /**
     * 배선을 소유한 설정 클래스를 그대로 올린다. 예전에는 factory 네 개를 직접 등록했는데,
     * 그러면 <b>조건이 클래스에 붙어 있다는 사실 자체를 테스트가 전제</b>하게 되어 배선 형태를
     * 바꾸는 순간 검사 대상이 사라진다. 지금은 운영이 쓰는 그 설정을 검사한다.
     */
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(AdminFixtureConfig.class);

    @Test
    @DisplayName("기본값에서는 fixture factory 가 하나도 등록되지 않는다")
    void fixtureFactoriesAreAbsentByDefault() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            for (FixtureBackedScreen screen : FIXTURE_BACKED_SCREENS) {
                assertThat(context)
                        .as("%s 화면의 fixture 가 기본값으로 등록된다. 운영 200 이 가짜 데이터를 낸다",
                                screen.screen())
                        .doesNotHaveBean(screen.mockFactory());
            }
        });
    }

    @Test
    @DisplayName("명시적으로 켜면 네 fixture factory 가 전부 등록된다")
    void fixtureFactoriesAppearWhenExplicitlyEnabled() {
        // 끄는 쪽만 보면 스위치 이름에 오타가 있어도 통과한다 — 어차피 아무것도 안 뜬다.
        runner.withPropertyValues("admin.mock.enabled=true").run(context -> {
            assertThat(context).hasNotFailed();
            for (FixtureBackedScreen screen : FIXTURE_BACKED_SCREENS) {
                assertThat(context)
                        .as("%s: 스위치를 켰는데 안 뜬다. 로컬·시연이 통째로 죽는다", screen.screen())
                        .hasSingleBean(screen.mockFactory());
            }
        });
    }

    @Test
    @DisplayName("fixture 를 끄면 그 화면의 Service 는 기동에서 실패한다 — 빈 결과로 위장하지 않는다")
    void disablingFixtureFailsFastInsteadOfReturningEmptyResults() {
        // 이 단언이 위 클래스 javadoc 의 "PENDING 이 아니라 기동 실패" 를 코드로 못박는다.
        // 누가 빈 결과를 내는 Pending 구현을 슬쩍 끼워 넣으면 여기가 깨진다 — 그 구현은
        // "조회했는데 없음" 과 구분되지 않아서, 지금의 시끄러운 실패보다 나쁘다.
        new ApplicationContextRunner()
                .withUserConfiguration(AdminFixtureConfig.class)
                .withBean(AdminIssuanceHistoryService.class)
                .run(context -> assertThat(context)
                        .as("fixture 없이도 컨텍스트가 떴다면, 그 화면은 무언가를 200 으로 내고 있다")
                        .hasFailed());
    }

    @Test
    @DisplayName("fixture 에 기대는 화면 목록이 실제 생성자와 같다")
    void inventoryMatchesActualConstructors() {
        // 계약이 두 곳(이 목록 ↔ Service 생성자)에 걸친다. 목록만 보는 테스트는 A 티켓이
        // Repository 를 붙였을 때 그 사실을 모르고, 생성자만 보는 테스트는 다섯 번째 화면이
        // 생겨도 모른다.
        // 두 배선 형태를 함께 받는다. 구상 클래스를 직접 무는 쪽(overview·issuancehistory·
        // couponmetrics)과, 포트를 물지만 그 포트의 구현이 mock factory 하나뿐인 쪽(inquiry)이다.
        // 후자는 analytics 와 겉모습이 같지만 대체 구현이 없어서 결과는 같다 — 끄면 못 뜬다.
        for (FixtureBackedScreen screen : FIXTURE_BACKED_SCREENS) {
            assertThat(constructorParameterTypes(screen.service()))
                    .as("%s 는 더 이상 fixture 를 안 문다. %s 가 실제 Repository 를 붙였다면 "
                                    + "이 목록에서 빼라 — 이 티켓이 남긴 빚이 그만큼 줄었다는 기록이다",
                            screen.service().getSimpleName(), screen.handoverTicket())
                    .anyMatch(parameter -> parameter.isAssignableFrom(screen.mockFactory()));
        }
    }

    @Test
    @DisplayName("스위치가 걸린 fixture factory 가 인벤토리와 정확히 같다")
    void inventoryCoversEverySwitchedFixtureFactory() {
        // ⚠️ 이 단언이 없으면 인벤토리에서 한 줄을 **지워도** 위 테스트들이 전부 통과한다 —
        //    목록을 순회할 뿐이라 대상이 줄어들 뿐 실패하지 않기 때문이다. 즉 빚을 갚지 않고
        //    장부만 지우는 길이 열린다. 그래서 배선 소스에서 직접 센다.
        List<String> switched = switchedFixtureFactoryNames();

        assertThat(switched)
                .as("AdminFixtureConfig 에서 %s 를 무는 @Bean 을 하나도 못 찾았다. 배선 형태가 "
                        + "바뀌었다면 이 스캔도 함께 바꿔야 한다", FIXTURE_SWITCH)
                .isNotEmpty();

        assertThat(switched)
                .as("스위치가 걸린 fixture factory 와 인벤토리가 어긋난다. 새 화면이 fixture 로 "
                        + "들어왔거나, 인계가 끝나지 않았는데 장부에서만 지웠다")
                .containsExactlyInAnyOrderElementsOf(
                        FIXTURE_BACKED_SCREENS.stream()
                                .map(screen -> screen.mockFactory().getSimpleName())
                                .toList());
    }

    @Test
    @DisplayName("배포자가 채우는 .env.example 이 두 fixture 스위치를 명시한다")
    void deploymentTemplateMakesTheFixtureSwitchesVisible() {
        // 코드 기본값이 꺼짐이어도, README 가 안내하는 유일한 배포 절차는
        // `cp application.yml.example application.yml` 이고 그 파일은 스위치를 켜 둔다.
        // 즉 **실제로 배포되는 값은 켜짐**이다 — A-06~A-11 이 실제 Repository 를 붙이기
        // 전까지는 그래야 화면이 뜨기 때문이고, 이 티켓이 바꾼 것은 그 켜짐이
        // "조건 없는 @Component" 가 아니라 **눈에 보이는 한 줄**이라는 점이다.
        //
        // 그 한 줄이 yml 의 기본값 표현식 안에만 있으면 배포자는 못 본다. 값을 채우는
        // 자리는 .env 다. 그래서 두 스위치가 거기 있는지, 그리고 yml 이 같은 이름을
        // 읽는지를 함께 본다 — 이름이 어긋나면 .env 를 고쳐도 아무 일이 안 일어난다.
        String env = read(repoRoot().resolve(".env.example"));

        for (String variable : List.of("ADMIN_MOCK_ENABLED", "ADMIN_ANALYTICS_MOCK_ENABLED")) {
            assertThat(env)
                    .as("%s 가 .env.example 에 없다. 배포자가 .env 만 채우고 application.yml 을 "
                            + "안 열면 가짜 데이터가 200 으로 나가는 것을 끝까지 모른다", variable)
                    .containsPattern("(?m)^" + variable + "=");

            for (String template : List.of("application.yml.example",
                    "api/src/main/resources/application.yml.example")) {
                assertThat(read(repoRoot().resolve(template)))
                        .as("%s 가 %s 를 안 읽는다. .env 의 그 줄이 아무 데도 안 닿는다",
                                template, variable)
                        .contains("${" + variable + ":");
            }
        }
    }

    @Test
    @DisplayName("analytics 는 이 목록에 없다 — 혼자 PENDING 을 낼 수 있기 때문이다")
    void analyticsIsNotFixtureBackedAtTheWiringLevel() {
        // analytics 도 기본값으로는 가짜 통계를 냈다(OBS-36 이 그 기본값을 뒤집었다). 다만
        // 배선 형태가 다르다 — Service 가 인터페이스를 물어서, 끄면 기동이 죽는 대신
        // AdminAnalyticsPendingSource 가 "아직 집계되지 않았음" 을 낸다.
        // 나머지 넷이 그 형태로 가려면 응답 계약을 새로 설계해야 하고, 그것이 A 티켓의 몫이다.
        assertThat(constructorParameterTypes(AdminAnalyticsService.class))
                .as("analytics 가 구상 Mock 을 직접 물기 시작했다면 위 인벤토리에 추가해야 한다")
                .noneMatch(type -> type.getName().contains(".mock."));
    }

    /** 배선이 스위치를 걸어 등록하는 fixture Factory 이름을 <b>설정 소스에서</b> 모은다. */
    private static List<String> switchedFixtureFactoryNames() {
        String source = read(repoRoot().resolve(
                "api/src/main/java/com/kafkick/api/admin/support/config/AdminFixtureConfig.java"));
        Matcher matcher = Pattern.compile(
                "@ConditionalOnProperty\\(name = FIXTURE_SWITCH[^)]*\\)\\s*"
                        + "public (\\w+) \\w+\\(").matcher(source);
        List<String> names = new ArrayList<>();
        while (matcher.find()) {
            names.add(matcher.group(1));
        }
        return names.stream().sorted().toList();
    }

    private static String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 작업 디렉터리가 모듈마다 달라 위로 올라가며 {@code settings.gradle} 로 찾는다. */
    private static Path repoRoot() {
        Path candidate = Path.of("").toAbsolutePath();
        while (candidate != null && !Files.isRegularFile(candidate.resolve("settings.gradle"))) {
            candidate = candidate.getParent();
        }
        if (candidate == null) {
            throw new IllegalStateException("settings.gradle 을 못 찾았다. 저장소 루트를 알 수 없다");
        }
        return candidate;
    }

    private static List<Class<?>> constructorParameterTypes(Class<?> type) {
        Constructor<?>[] constructors = type.getDeclaredConstructors();
        assertThat(constructors)
                .as("%s 의 생성자가 여럿이다. 어느 것이 배선용인지 정해야 이 검사가 뜻을 갖는다",
                        type.getSimpleName())
                .hasSize(1);
        return Arrays.asList(constructors[0].getParameterTypes());
    }
}
