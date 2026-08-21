// 배치 메타데이터를 DB 에 남깁니다.
package com.kafkick.batch.config;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.Isolation;

/**
 * <b>이것이 없으면 배치 메타데이터가 어디에도 안 남는다.</b>
 *
 * <p>Spring Batch 6 의 기본 {@code JobRepository} 는 {@code ResourcelessJobRepository} 이고
 * Boot 4 의 배치 자동설정은 그것을 그대로 쓴다. 즉 <b>아무 설정도 안 하면 메모리조차 아니고
 * 그냥 안 남는다.</b> 실측으로 확인했다 — 배선 전에 잡을 한 번 돌리고 센 값이다.
 *
 * <pre>
 *   repoClass  = org.springframework.batch.core.repository.support.ResourcelessJobRepository
 *   instanceId = 1            ← 파라미터가 무엇이든 항상 1
 *   BATCH_JOB_INSTANCE = 0    BATCH_JOB_EXECUTION = 0    BATCH_STEP_EXECUTION = 0
 * </pre>
 *
 * <p><b>그 상태에서는 이 저장소가 적어 둔 것 여럿이 거짓이 된다.</b>
 *
 * <ul>
 *   <li>{@code ResourcelessJobRepository.isJobInstanceExists} 가 <b>항상 false</b> 라
 *       "같은 파라미터의 완료된 실행은 다시 못 돌린다" 는 중복 방지가 통째로 없다 —
 *       {@code ExpireScheduler} 가 {@code asOf} 를 분 단위로 자르는 근거가 그것이었다</li>
 *   <li>{@code JobExecutionAlreadyRunningException} ·
 *       {@code JobInstanceAlreadyCompleteException} 을 잡는 갈래가 도달 불가능해진다</li>
 *   <li>{@code V2__batch_metadata.sql} 이 만든 아홉 테이블이 영원히 비어 있고,
 *       "언제 몇 건을 넘겼나" 를 볼 곳이 없다. 알림 규칙이 안내하는
 *       {@code BATCH_STEP_EXECUTION.WRITE_COUNT} 조회도 항상 0행이다</li>
 * </ul>
 *
 * <p><b>빈을 직접 정의하는 대신 이 애너테이션을 쓴다.</b> Boot 의
 * {@code BatchAutoConfiguration$SpringBootBatchDefaultConfiguration.jobRepository()} 는
 * {@code @ConditionalOnMissingBean} 이 아니라서, 같은 이름의 빈을 정의하면 물러나는 것이
 * 아니라 {@code BeanDefinitionOverrideException} 으로 <b>기동이 깨진다</b>(직접 겪었다).
 * 이 애너테이션은 {@code DefaultBatchConfiguration} 을 들여와 자동설정 전체를 물러나게 한다.
 *
 * <p><b>{@code isolationLevelForCreate} 를 {@link Isolation#SERIALIZABLE} 로 둔다.</b>
 * 인스턴스를 만드는 그 순간만 직렬화하면 두 프로세스가 같은 파라미터로 동시에 시작하는 것을
 * DB 가 막는다 — 배치를 여러 대로 늘리는 날 필요한 것이 그 한 점이다.
 * 만료 Step 자체의 격리(READ COMMITTED)와는 별개다.
 */
@Configuration(proxyBeanMethods = false)
@EnableBatchProcessing
@EnableJdbcJobRepository(isolationLevelForCreate = Isolation.SERIALIZABLE)
public class BatchJobRepositoryConfig {
}
