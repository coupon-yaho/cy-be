// 검증·시드·만료 배치를 실행하는 별도 프로세스입니다. api 와 같은 DB 를 보되 커넥션 풀은 분리됩니다.
package com.kafkick;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
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
@SpringBootApplication(scanBasePackages = "com.kafkick")
@EnableScheduling
public class BatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

}
