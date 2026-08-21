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
 * 직접 정의해야 하는 것은 아니다 — @EnableBatchProcessing 이 스스로 등록한다. 오히려
 * 직접 정의하면 Boot 쪽 빈과 이름이 부딪혀 기동이 깨진다.
 * 잃는 것과 안 잃는 것은 BatchJobRepositoryConfig 의 javadoc 에 적었다.
 */
@SpringBootApplication(scanBasePackages = "com.kafkick")
@EnableScheduling
public class BatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

}
