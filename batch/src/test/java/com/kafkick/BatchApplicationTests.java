package com.kafkick;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;

import com.kafkick.core.admin.overview.AdminOverviewService;
import com.kafkick.core.coupon.v2.V2StockRestorationService;
import com.kafkick.storage.db.MySqlContainerConfig;

// JPA 스키마 검증이 켜져 있다(storage.yml 의 ddl-auto: validate). batch 는 마이그레이션
// 소유자가 아니라 spring.flyway.enabled 가 false 인데, 테스트 컨테이너는 빈 DB 로 뜨므로
// 여기서만 켜서 스키마를 만든다 — 배치 메타 테스트들이 이미 쓰는 패턴이다.
@SpringBootTest(properties = "spring.flyway.enabled=true")
@Import(MySqlContainerConfig.class)
class BatchApplicationTests {

    @Autowired
    private ApplicationContext context;

    /** API 전용 Overview 원천이 없는 Batch 전체 Context는 Overview Service 없이 기동합니다. */
    @Test
    void startsBatchContextWithoutAdminOverviewService() {
        assertThat(context.getBeansOfType(AdminOverviewService.class)).isEmpty();
    }

    /**
     * <b>[OBS-36] 관리자 화면 빈이 batch 에 하나도 없다.</b>
     *
     * <p>위 단언만 있을 때는 <b>우연히</b> 통과하고 있었다 — {@code AdminOverviewService} 는
     * {@code @Service} 가 아니라 api 의 {@code @Bean} 이라 애초에 스캔에 안 걸렸을 뿐이고,
     * 형제인 발급문의·발급이력·상세지표 서비스는 {@code @Service} 라 <b>매 기동마다 batch 에
     * 만들어지고 있었다.</b> 그 사실은 fixture 스위치를 끄자 batch 가 기동에서 죽으며 드러났다.
     *
     * <p>지금은 {@code BatchApplication} 의 {@code @ComponentScan} 이 그 패키지를 통째로 뺀다.
     * 그 제외가 지워지면 여기가 깨진다 — 이름 하나가 아니라 <b>패키지</b>로 보기 때문에,
     * 새 관리자 서비스가 늘어도 같은 그물에 걸린다.
     */
    @Test
    void startsBatchContextWithoutAnyAdminBean() {
        List<String> adminBeans = Arrays.stream(context.getBeanDefinitionNames())
                .filter(name -> {
                    Class<?> type = context.getType(name);
                    return type != null && type.getName().startsWith("com.kafkick.core.admin.");
                })
                .toList();

        assertThat(adminBeans)
                .as("batch 는 core.admin 을 본 코드에서 한 줄도 참조하지 않는다. "
                        + "그 패키지의 빈이 여기 있다면 스캔 제외가 풀린 것이다")
                .isEmpty();
    }

    /**
     * <b>형제 제외에도 같은 그물을 단다(CY-744).</b> 위 단언은 {@code core.admin} 만 보므로,
     * 새로 붙인 {@code core.coupon.service} 제외가 풀려도 아무것도 안 깨졌다 —
     * 이 클래스 자신이 <i>"위 단언만 있을 때는 우연히 통과하고 있었다"</i> 고 적어 둔
     * 그 실수를 그대로 반복한 것이다.
     *
     * <p><b>풀렸을 때의 실패가 조용할 수 있다.</b> 제외가 없으면
     * {@code IdempotencyExecutionService} 가 {@code coupon.idempotency.*} 를 요구해
     * 기동이 죽지만, 그건 <b>그 설정이 없을 때</b>뿐이다. 누가 batch 설정에 그 키를 넣는 날
     * 필터가 풀려도 배치는 뜨고 <b>쓰지도 않을 @Service 열일곱이 매 기동마다 만들어진다.</b>
     */
    @Test
    void startsBatchContextWithoutAnyCouponServiceBean() {
        List<String> couponServiceBeans = Arrays.stream(context.getBeanDefinitionNames())
                .filter(name -> {
                    Class<?> type = context.getType(name);
                    return type != null
                            && type.getName().startsWith("com.kafkick.core.coupon.service.");
                })
                .toList();

        assertThat(couponServiceBeans)
                .as("batch 는 core.coupon.service 를 본 코드에서 한 줄도 참조하지 않는다. "
                        + "만료·회차는 이 저장소에서 Spring Batch 잡이 진다 — "
                        + "그 패키지의 빈이 여기 있다면 BatchApplication 의 스캔 제외가 풀린 것이다")
                .isEmpty();
    }

    /**
     * <b>만료가 쓰는 복원 서비스가 실물로 있다.</b> 위 두 단언이 "batch 에 없어야 할 것" 을
     * 재는 그물이라면, 이것은 <b>있어야 할 것</b>을 재는 반대쪽 그물이다.
     *
     * <p>이 검사가 필요한 이유는 이력이다. {@code V2StockRestorationService} 는 한때
     * {@code core.coupon.service} 에 있었는데, CY-744 가 그 패키지를 batch 스캔에서 빼면서
     * {@code expireStep} 이 주입받을 빈이 사라졌다 — <b>컨텍스트가 통째로 안 뜨고
     * batch 테스트 378 개가 한꺼번에 빨개졌다.</b>
     *
     * <p>그리고 그 회귀를 <b>{@code ExpireV2StockRestorationTest} 가 못 잡았다.</b> 그쪽은
     * 호출을 검증하려고 {@code @MockitoBean} 을 물리는데, 그 애노테이션은 <b>없는 빈이면
     * 새로 만들어 준다.</b> 배선 구멍이 정확히 그 자리에서 덮인다. 그래서 실물의 존재는
     * 목을 안 쓰는 이 컨텍스트에서 따로 잰다.
     */
    @Test
    void startsBatchContextWithV2StockRestorationService() {
        assertThat(context.getBeansOfType(V2StockRestorationService.class))
                .as("만료 잡이 이 빈을 생성자로 받는다. 없으면 expireStep 부터 조립이 깨져 "
                        + "batch 컨텍스트가 통째로 안 뜬다 — @MockitoBean 을 쓰는 테스트는 "
                        + "그 구멍을 덮으므로 여기서 잰다")
                .isNotEmpty();
    }

}
