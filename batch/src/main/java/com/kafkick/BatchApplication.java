// 검증·시드·만료 배치를 실행하는 별도 프로세스입니다. api 와 같은 DB 를 보되 커넥션 풀은 분리됩니다.
package com.kafkick;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * scanBasePackages 를 com.kafkick 로 넓힌 이유 — 이 클래스가 com.kafkick 에 있어 기본 스캔은
 * 같은 범위지만, storage 모듈의 설정(JpaAuditConfig 등)과 core 의 TimeProvider 가
 * com.kafkick.storage / com.kafkick.core 에 있으므로 명시해 의도를 남긴다.
 *
 * <b>@EnableBatchProcessing 은 여기 안 붙인다 — {@code BatchJobRepositoryConfig} 가 붙인다.</b>
 * 예전에 이 자리에 "붙이지 않는다" 고 적혀 있었는데, 그러면 JobRepository 가
 * ResourcelessJobRepository 로 남아 BATCH_* 아홉 테이블이 영원히 빈다(실측으로 확인했다).
 *
 * <p>붙이면 BatchAutoConfiguration 이 물러나는 것은 맞다. 다만 JobRepository·JobOperator 를
 * 직접 정의해야 하는 것은 아니다 — @EnableBatchProcessing 이 스스로 등록한다.
 *
 * <p><b>그리고 직접 정의하면 기동이 깨지는 것이 아니라 조용히 이긴다.</b> BatchRegistrar 는
 * 같은 이름의 빈 정의가 이미 있으면 debug 한 줄을 남기고 물러난다 — 그러면 기동은 성공하고
 * 메타는 다시 0행이 된다. 이 티켓이 존재하는 이유가 그 조용한 실패다.
 * 기동이 깨지는 것은 @EnableBatchProcessing 을 <b>안 붙인 채</b> Boot 쪽 jobRepository 와
 * 이름이 겹칠 때다.
 *
 * <p><b>[CY-338] 원문은 {@code feature/CY-15} 것이다.</b> {@code BatchJobRepositoryConfig} 만
 * 가져오고 그 짝인 이 설명을 안 가져와서, 한동안 이 파일이 <i>"붙이지 않는다"</i> 라고
 * 말하는 동안 실제로는 붙어 있었다. 그 경로를 지키는 것은 CY-15 에서는
 * {@code BatchJobRepositoryTest}, 이 브랜치에서는 {@code BatchMetadataPersistenceTest} 다.
 *
 * <p>붙여서 잃는 것과 안 잃는 것은 docs/11-batch-implementation.md 에 적었다.
 */
/*
 * [OBS-36] 관리자 화면(core.admin)을 스캔에서 뺀다.
 *
 * batch 는 그 패키지를 **본 코드에서 한 줄도 참조하지 않는다**(확인함). 그런데 셋이 @Service 라
 * 스캔에 걸려, 배치 JVM 이 쓰지도 않을 AdminIssuanceInquiry·AdminIssuanceHistory·
 * AdminCouponMetrics 서비스와 그 fixture Factory 를 매 기동마다 만들고 있었다.
 * AdminOverviewService 만 우연히 빠져 있었는데(그것은 @Service 가 아니라 api 의 @Bean 이다),
 * BatchApplicationTests 가 그 우연을 계약처럼 단언하고 있었다.
 *
 * 드러난 계기는 fixture 스위치다 — admin.mock.enabled 를 기본 꺼짐으로 바꾸자 **batch 가 기동에서
 * 죽었다**. 관리자 화면과 아무 상관 없는 프로세스가 그 화면의 fixture 에 매여 있었다는 뜻이다.
 * batch 쪽 설정에 스위치를 켜서 덮으면 그 결합이 그대로 남으므로, 결합을 끊는다.
 *
 * ⚠️ Spring Boot 4.1 의 @SpringBootApplication 에는 excludeFilters 속성이 **없다**(실측:
 *    "cannot find symbol: method excludeFilters()"). 그래서 @ComponentScan 을 따로 단다.
 *
 * ⚠️ excludeFilters 를 지정하면 @SpringBootApplication 의 **기본 필터 둘이 사라진다.**
 *    그래서 여기 다시 적는다. 두 필터가 무엇을 거르는지는 Boot 가 정하는 것이라 이 코드가
 *    보장하지 않는다 — 여기서 보장하는 것은 **기본값을 복원한다**는 것 하나다.
 *    빼도 되는지 확인하지 않은 채 빼지 말 것.
 */
@SpringBootApplication(scanBasePackages = "com.kafkick")
@ComponentScan(
        basePackages = "com.kafkick",
        excludeFilters = {
                @Filter(type = FilterType.CUSTOM, classes = TypeExcludeFilter.class),
                @Filter(type = FilterType.CUSTOM, classes = AutoConfigurationExcludeFilter.class),
                @Filter(type = FilterType.REGEX, pattern = "com\\.kafkick\\.core\\.admin\\..*")
        })
@EnableScheduling
public class BatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

}
