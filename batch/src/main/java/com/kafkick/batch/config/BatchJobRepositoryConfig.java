// 배치 메타데이터를 DB 에 남깁니다.
package com.kafkick.batch.config;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.Isolation;

/**
 * <b>이것이 없으면 배치 메타데이터가 어디에도 안 남는다.</b>
 *
 * <p><b>[CY-338 이관 주석] 이 파일은 {@code feature/CY-15} 에서 그대로 가져왔다.</b>
 * 아래 설명 중 <b>이 브랜치에서 아직 성립하지 않는 것</b>을 먼저 적는다 — 주석이 없는 코드를
 * 가리키면 읽는 사람이 그것을 찾다가 시간을 버린다.
 *
 * <ul>
 *   <li>{@code ExpireScheduler} · {@code expireJob} · {@code verifyJob} 은 <b>이 브랜치에 없다</b>
 *       (CY-15 소유). 여기서는 잡이 아직 하나도 없고, 이 설정은 <b>배치 메타 테이블을
 *       채우기 위해</b>서만 들어왔다 — 그것이 없으면 이력 조회 API 가 항상 빈 목록을
 *       돌려주면서 테스트까지 통과한다</li>
 *   <li>격리 수준 실측표(MySQL 26.7.0)는 CY-15 에서 측정한 값이다. 이 브랜치에서 다시
 *       재지 않았다 — 같은 스키마({@code V2__batch_metadata.sql} 가 두 계열에서 동일)라
 *       그대로 믿는다</li>
 * </ul>
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
 * <p><b>중복 방지의 실체는 UNIQUE 인덱스다 — 그리고 격리를 내려야 그것이 일한다.</b>
 * 예전에 {@code isolationLevelForCreate = SERIALIZABLE} 을 적고 <i>"그 순간만 직렬화하면
 * 두 프로세스가 동시에 시작하는 것을 DB 가 막는다"</i> 고 설명했다가, 그 값이 애너테이션
 * <b>기본값</b>이라 아무것도 안 바꾼다는 것을 확인하고 인자를 지웠다. <b>그것도 틀렸다</b> —
 * 지우면 기본값 SERIALIZABLE 이 그대로 산다.
 *
 * <p>MySQL 에서 SERIALIZABLE 은 평범한 {@code SELECT} 를 잠금 읽기로 올린다.
 * {@code JdbcJobInstanceDao} 는 <i>"있는지 SELECT → 없으면 INSERT"</i> 라, 그 SELECT 가
 * {@code JOB_INST_UN} 에 gap 락을 잡는다. 실측(MySQL 26.7.0, 두 세션 동시):
 *
 * <pre>
 *                        같은 잡·같은 키        다른 잡·다른 키
 *   SERIALIZABLE         오류 1213 (데드락)     오류 1213 · 한쪽이 통째로 실패
 *   READ COMMITTED       오류 1062 (중복 키)    둘 다 성공
 * </pre>
 *
 * <p><b>오른쪽 열이 문제다.</b> {@code expireJob} 과 {@code verifyJob} 은 이름도 키도 다른데
 * 같은 gap 에 들어가면 서로 죽인다 — 중복 방지와 아무 상관 없는 조합이고,
 * {@code BATCH_JOB_INSTANCE} 가 비어 있을수록(첫 배포·정리 직후) 전부 같은 gap 이다.
 * 그때(= {@code SERIALIZABLE} 로 두면) 지는 쪽은 {@code DeadlockLoserDataAccessException} 을
 * 받아 {@code ExpireScheduler} 의 ERROR 갈래로 나간다.
 *
 * <p><b>지금은 그 타입이 오지 않는다.</b> 아래처럼 내려 뒀으므로 지는 쪽이 받는 것은
 * {@code DuplicateKeyException} · {@code IllegalStateException} ·
 * {@code JobInstanceAlreadyCompleteException} 셋 중 하나이고, 셋 다 그쪽에서 INFO 로 받는다.
 * 그 계약을 {@code BatchMetadataPersistenceTest} 가 고정한다 —
 * {@code DeadlockLoser} 가 오면 격리가 기본값으로 되돌아간 것이라 빨간불이 뜬다.
 *
 * <p>그래서 <b>내려서 명시한다.</b> 중복 방지는 {@code V2__batch_metadata.sql} 의
 * {@code JOB_INST_UN UNIQUE (JOB_NAME, JOB_KEY)} 가 하고, 두 번째 INSERT 가 1062 로 거부된다.
 * 그 인덱스를 지우면 두 노드가 같은 {@code asOf} 로 각자 인스턴스를 만든다.
 *
 * <p><b>{@code V2__batch_metadata.sql} 의 머리말은 낡았다.</b> 그 파일은
 * {@code spring.batch.jdbc.initialize-schema: never} 를 근거로 대는데 <b>Boot 4.1 에 그
 * 프로퍼티 그룹이 없다</b>({@code BatchProperties} 의 중첩 클래스가 {@code Job(name)} 하나뿐이다).
 * 그리고 그 파일이 없어도 <b>기동은 성공한다</b> — 첫 잡 실행에서
 * {@code Table 'BATCH_JOB_INSTANCE' doesn't exist} 로 죽는다. 이미 적용된 마이그레이션이라
 * 체크섬 때문에 손대지 않고 정정을 여기 둔다.
 *
 * <p><b>{@code JobOperator} 가 동기로 돌아야 한다 — 그런데 그것을 여기서 못 박지 않는다.</b>
 * {@code BatchRegistrar} 는 이름이 {@code taskExecutor} 인 빈 <b>정의가 있으면</b> 그것을 쓰고,
 * 없으면 동기로 떨어진다. Boot 4 는 {@code applicationTaskExecutor} 로만 등록하고 그 별칭을
 * 안 붙여서 지금은 동기다.
 *
 * <p><b>{@code taskExecutorRef} 로 전용 {@code SyncTaskExecutor} 를 물려 봤다가 되돌렸다.</b>
 * {@code Executor} 타입 빈이 하나 생기는 순간 Boot 의
 * {@code TaskExecutorConfigurations} 가 {@code @ConditionalOnMissingBean(Executor.class)} 에서
 * 떨어져 <b>{@code applicationTaskExecutor} 가 통째로 사라진다</b>(실측했다). 그러면 MVC 비동기가
 * 요청당 스레드로 폴백하고 {@code @Async} 가 무제한으로 돌며 {@code spring.task.execution.*} 이
 * 죽는다 — 배치 불변식 하나 지키려고 웹 계층 기본값을 날리는 거래다.
 *
 * <p>그래서 배선을 건드리는 대신 <b>{@code BatchJobRepositoryTest} 가 그 이름의 빈이 없다는
 * 것을 단언한다.</b> 누가 {@code @Bean("taskExecutor")} 를 넣으면 — 스프링 예제가 관례로 쓰는
 * 이름이다 — 거기서 빨간불이 뜬다.
 *
 * <p><b>[CY-338] 그 테스트는 {@code feature/CY-15} 에 있고 이 브랜치에는 없다.</b>
 * 여기서 같은 이름으로 만들면 합류 때 같은 경로에서 부딪히므로 만들지 않았다 —
 * 즉 <b>이 브랜치에서 그 방어선은 비어 있다.</b> 합류하면 그쪽 것이 그대로 이 자리를 맡는다.
 *
 * <p><b>배선의 대가</b> — 이제 {@code BATCH_*} 가 실제로 쌓인다. 만료가 5분마다 새 {@code asOf}
 * 로 도므로 하루 288 인스턴스 × 여섯 테이블이다. 정리 경로는 아직 없다(cleanup 티켓에서 본다).
 */
/*
 * ── 아래 둘은 CY-15 원본에 없다. 이 브랜치의 풀 배선이 달라서 붙였다 ──
 *
 * dataSourceRef = "mainDataSource"
 *   기본값은 "dataSource" 인데 이 브랜치에는 그 이름의 빈이 없다. 실측한 빈 이름:
 *     관측 ON  → mainDataSource(@Primary) · observationDataSource(@Qualifier("obs"))
 *     관측 OFF → 없음
 *   기본값 그대로 두면 "No bean named 'dataSource' available" 로 batch 기동이 깨진다
 *   (BatchApplicationTests 가 실제로 그렇게 죽었다).
 *
 *   ⚠️ 관측 풀(obs)이 아니라 운영 풀이어야 한다. 배치 메타는 쓰기이고 obs 계정은 SELECT 전용이다.
 *
 * ── 조건이 없다. 예전에는 있었고, 그것이 결함이었다 ──
 *
 *   {@code @ConditionalOnProperty("observation.datasource.enabled")} 를 달았었다.
 *   그때는 storage 의 ObservationDataSourceConfig 가 운영 풀 정의까지 들고 물러나서,
 *   조건 없이 두면 이 설정이 DataSource 를 못 찾아 기동이 깨졌기 때문이다.
 *
 *   ⚠️ 그 조건의 대가가 훨씬 컸다 — 정상 운영 스위치 하나로 <b>잡 중복 실행 방지가 통째로
 *      꺼졌다.</b> Spring Batch 가 ResourcelessJobRepository 로 떨어져 JOB_INST_UN 에
 *      INSERT 가 가지 않고, 두 노드가 같은 파라미터로 각자 잡을 시작할 수 있게 된다.
 *      예외도 로그도 없다.
 *
 *   그래서 원인을 없앴다 — 운영 풀을 {@code MainDataSourceConfig} 로 떼어 내 관측 스위치와
 *   무관하게 항상 만들어지게 했다. 이 설정은 이제 조건이 필요 없다.
 *   {@code BatchMetadataWithoutObservationTest} 가 그 분리를 고정한다.
 *
 *   ⚠️ 처음에 {@code @ConditionalOnBean(name = "mainDataSource")} 로 썼다가 되돌린 기록은
 *      남겨 둔다 — <b>조건이 항상 false 로 평가돼 이 설정이 통째로 안 붙었는데
 *      :batch:test 87개가 전부 초록불이었다.</b> @ConditionalOnBean 은 자동설정 클래스에서만
 *      순서가 보장되고, 컴포넌트 스캔되는 @Configuration 에서는 대상 빈 정의가 등록되기 전에
 *      평가된다. 조건을 다시 붙이려는 사람이 같은 자리를 밟지 않게 적어 둔다.
 */
@Configuration(proxyBeanMethods = false)
@EnableBatchProcessing
@EnableJdbcJobRepository(
        dataSourceRef = "mainDataSource",
        isolationLevelForCreate = Isolation.READ_COMMITTED)
public class BatchJobRepositoryConfig {
}
