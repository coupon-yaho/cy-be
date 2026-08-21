// 배치 메타데이터를 DB 에 남깁니다.
package com.kafkick.batch.config;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;

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
 * <p><b>중복 방지의 실체는 격리 수준이 아니라 UNIQUE 인덱스다.</b> 예전에 여기
 * {@code isolationLevelForCreate = SERIALIZABLE} 을 적고 <i>"인스턴스를 만드는 그 순간만
 * 직렬화하면 두 프로세스가 동시에 시작하는 것을 DB 가 막는다"</i> 고 설명했는데, <b>그 값은
 * 애너테이션 기본값</b>이라 아무것도 안 바꾼다(바이트코드의 {@code AnnotationDefault} 확인).
 * 실제로 막는 것은 {@code V2__batch_metadata.sql} 의
 * {@code JOB_INST_UN UNIQUE (JOB_NAME, JOB_KEY)} 이고, 두 번째 INSERT 가 1062 로 거부된다.
 * 그 인덱스를 지우면 두 노드가 같은 {@code asOf} 로 각자 인스턴스를 만든다.
 *
 * <p>그래서 인자를 명시하지 않는다 — 기본값을 다시 적으면 손잡이처럼 보이고, 방어를 엉뚱한
 * 곳에 귀속시킨다. 만료 Step 자체의 격리(READ COMMITTED)와도 별개다.
 *
 * <p><b>배선의 대가</b> — 이제 {@code BATCH_*} 가 실제로 쌓인다. 만료가 5분마다 새 {@code asOf}
 * 로 도므로 하루 288 인스턴스 × 여섯 테이블이다. 정리 경로는 아직 없다(cleanup 티켓에서 본다).
 */
@Configuration(proxyBeanMethods = false)
@EnableBatchProcessing(taskExecutorRef = "batchTaskExecutor")
@EnableJdbcJobRepository
public class BatchJobRepositoryConfig {

    /**
     * <b>{@code JobOperator.start} 가 동기로 돌아야 한다.</b> {@code ExpireScheduler} 의 겹침
     * 방지가 그 위에 서 있다 — 크론 트리거는 앞 실행이 <b>끝난 뒤에야</b> 다음을 잡으므로,
     * {@code start} 가 즉시 {@code STARTED} 를 돌려주면 그 보호가 통째로 사라지고 만료가
     * 자기 자신과 겹쳐 같은 구간을 서로 X 락으로 잡는다.
     *
     * <p><b>지금까지 그것을 지킨 것은 우연이었다.</b> {@code BatchRegistrar} 는 이름이
     * {@code taskExecutor} 인 빈 <b>정의가 있으면</b> 그것을 쓰고, 없으면 동기로 떨어진다.
     * Boot 4 는 {@code applicationTaskExecutor} 로만 등록하고 그 별칭을 안 붙여서 지금은
     * 동기다. 누가 {@code @Bean("taskExecutor")} 를 하나 넣는 순간 — 그 이름은 스프링 예제가
     * 관례로 쓰는 이름이다 — 컴파일도 기동도 성공한 채로 겹침 방지가 죽는다.
     *
     * <p>그래서 전제를 이름 우연에서 떼어 내 여기 못 박는다.
     */
    @Bean
    SyncTaskExecutor batchTaskExecutor() {
        return new SyncTaskExecutor();
    }
}
