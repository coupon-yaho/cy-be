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

}
