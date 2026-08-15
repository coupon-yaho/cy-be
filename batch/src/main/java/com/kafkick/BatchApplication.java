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
 * @EnableBatchProcessing 은 붙이지 않는다. Spring Boot 3+ 에서는 이걸 붙이는 순간
 * BatchAutoConfiguration 이 물러나 JobRepository·JobLauncher 를 직접 정의해야 한다.
 */
@SpringBootApplication(scanBasePackages = "com.kafkick")
@EnableScheduling
public class BatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

}
