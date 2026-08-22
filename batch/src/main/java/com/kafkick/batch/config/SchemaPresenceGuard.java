// 배치가 보는 스키마에 핵심 테이블이 있는지 기동 시점에 확인합니다.
package com.kafkick.batch.config;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.VerificationRuleRepository;
import com.kafkick.core.verification.exception.VerificationErrorCode;

/**
 * <b>batch 는 스키마를 만들지 않는다. 그런데 없어도 기동이 성공한다 — 그게 문제다.</b>
 *
 * <p>마이그레이션 소유자는 {@code api} 하나로 고정돼 있고 batch 는 {@code flyway.enabled:false}
 * 다({@code application.yml.example}). 대가는 <b>배포 순서 의존</b>인데 그 위반이 조용하다 —
 * batch 에는 {@code @Entity} 가 없어 {@code ddl-auto: validate} 가 공허하게 통과하고, 메타
 * 테이블 존재도 기동 때는 아무도 안 본다({@code docs/11} 의 "{@code application.yml} 은
 * 문서가 둘이다" 절이 그 대가를 적어 뒀다).
 *
 * <p><b>compose 로도 못 막는다.</b> {@code batch.yml} 의
 * {@code depends_on: mysql: condition: service_healthy} 가 보장하는 것은 {@code mysqladmin ping}
 * 성공, 즉 <b>mysqld 가 살아 있다</b> 뿐이다. 마이그레이션이 끝났다는 뜻이 아니고,
 * {@code base.yml} 에는 그것을 돌리는 {@code api} 서비스도 없다.
 *
 * <p><b>CY-359 가 그 침묵을 한 단계 더 나쁘게 만들었다.</b> {@link VerificationMetrics} 는
 * 자기가 어느 데이터셋을 보는지를 {@code rules.hasCleanOnlyConstraints()} 로 정하는데, 그
 * 구현은 {@code uk_coupon_member} 인덱스를 묻는 {@code EXISTS} 라 <b>테이블이 하나도 없으면
 * 예외 없이 {@code false}</b> 를 준다. 그러면 빈 DB 에 붙은 프로세스가
 * {@code cy_verification_verdict{dataset="CORRUPT"}} 를 내보낸다 — <i>"스키마가 없다"</i> 가
 * <i>"CORRUPT 셋 검증이 안 돌았다"</i> 로 읽힌다. 관제에 나가는 라벨이 틀리는 것이라
 * 늦게 발견될수록 비싸다.
 *
 * <p><b>왜 {@link ApplicationRunner} 인가.</b> 컨텍스트 refresh 가 끝난 뒤에 돌아
 * {@code FlywayMigrationInitializer} 보다 확실히 뒤에 온다. {@code InitializingBean} 은 그
 * 순서가 보장되지 않는다 — {@code docs/11} 이 이 가드를 예약하면서 근거까지 적어 둔 결정이고,
 * 여기서는 그것을 그대로 따른다.
 *
 * <p><b>이 가드가 보는 것은 "테이블이 있나" 지 "스키마가 최신인가" 가 아니다.</b> 목록을
 * 넓히면 스키마가 자랄 때마다 기동이 막혀 Flyway 의 몫을 뺏는다. 그래서 배치가 없으면
 * 아무것도 못 하는 것만 본다 — 데이터 넷과 Spring Batch 메타 넷이다.
 *
 * <p><b>메타 축이 데이터 축과 따로 빈다.</b> 검증용 셋({@code coupon_clean}·
 * {@code coupon_corrupt})은 cy-seed 의 {@code ddl/} 로 만들어지는데 거기에
 * {@code BATCH_*} 가 <b>하나도 없다</b>. 그 DB 를 보게 배치를 띄우는 것이 정상 절차이므로
 * (설정 파일이 그 절차를 문서화해 뒀다), 데이터 넷은 다 있고 메타만 없는 상태가 실제로
 * 생긴다. 그때 기동은 통과하고 <b>첫 잡 실행에서</b>
 * {@code Table 'BATCH_JOB_INSTANCE' doesn't exist} 로 죽는다 — {@code docs/11} 이 배포
 * 순서 위반의 증상으로 지목한 바로 그 문자열이고, 이 가드가 없애려는 늦은 실패다.
 *
 * <p><b>넣자마자 실제로 하나를 잡았다.</b> {@code ActuatorExposureTest} 와
 * {@code ActuatorWildcardExposureTest} 가 {@code spring.config.location} 을
 * {@code resolved/application.yml} 하나로 지정해 테스트용 {@code application.yml}
 * ({@code flyway.enabled:true})을 덮고 있었다 — <b>빈 스키마 위에서 뜨고 있었다</b>.
 * 의도가 아니라 부작용이었고, 그 상태로는 두 테스트가 실제 기동과 다른 것을 재고 있었다.
 *
 * <p><b>{@link CleanSchemaGuard} 와 다른 축이다.</b> 저쪽은 <i>어느 스키마를 보고 있나</i>
 * (CLEAN/CORRUPT)를 잡 시작 전에 묻고, 이쪽은 <i>스키마가 있기는 한가</i> 를 기동 시점에
 * 묻는다. 저쪽이 성립하려면 이쪽이 먼저 참이어야 한다.
 */
@Component
// 잡보다 먼저 돌아야 한다. JobLauncherApplicationRunner 의 순서가 0 이라, 정렬값을 안 주면
// LOWEST_PRECEDENCE 로 그 뒤에 온다 — 가드가 말하기 전에 잡이 SQL 에러로 죽는다.
// 지금은 spring.batch.job.enabled 가 false 라 안 터지지만, 그 결합을 설정 한 줄에
// 맡겨 두지 않는다. Flyway 는 FlywayMigrationInitializer(빈 초기화 단계)라 러너 순서와
// 경쟁하지 않으므로 위 javadoc 의 근거는 그대로 산다.
@Order(Ordered.HIGHEST_PRECEDENCE)
public class SchemaPresenceGuard implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SchemaPresenceGuard.class);

    private static final String META_PREFIX = "BATCH_";

    private final VerificationRuleRepository rules;

    public SchemaPresenceGuard(VerificationRuleRepository rules) {
        this.rules = rules;
    }

    @Override
    public void run(ApplicationArguments args) {
        String schema = rules.currentSchema();
        List<String> missing = rules.missingCoreTables();
        if (!missing.isEmpty()) {
            throw new BusinessException(VerificationErrorCode.SCHEMA_NOT_MIGRATED,
                    message(schema, missing));
        }
        // 테이블이 다 있다고 컬럼도 있는 것은 아니다. cy-seed 1f217b5 이전에 만든 검증용
        // 셋이 정확히 그 모양이다 — 그대로 띄우면 기동은 통과하고 되읽기가 매 주기
        // Unknown column 으로 실패한다. 게이지가 직전 값을 유지하므로 조용하고,
        // 알림이 뜨기까지 최소 15분이다. 여기서 잡으면 즉시, 조치까지 함께 말한다.
        List<String> missingColumns = rules.missingCriticalColumns();
        if (!missingColumns.isEmpty()) {
            throw new BusinessException(VerificationErrorCode.SCHEMA_NOT_MIGRATED,
                    "배치가 보는 스키마(" + schema + ")에 컬럼이 없습니다: "
                            + String.join(", ", missingColumns)
                            + ". 테이블은 전부 있으므로 배포 순서 문제가 아닙니다 — "
                            + "cy-seed 1f217b5 이전에 만든 검증용 셋입니다. Flyway 가 그 DB 에 "
                            + "닿지 않아 마이그레이션으로는 못 고칩니다. 데이터셋을 다시 만드십시오.");
        }
        log.info("스키마 확인 완료 — 배치 핵심 테이블과 컬럼이 전부 있습니다. schema={}", schema);
    }

    /**
     * <b>같은 증상이 세 원인을 갖는다.</b> 조치가 각각 다른데 한 문장으로 접으면 사람이
     * 엉뚱한 것을 재배포하며 시간을 쓴다 — 이 저장소가 반복해서 없애 온
     * <i>"0건이 두 뜻을 갖는다"</i> 와 같은 형태다.
     *
     * <ul>
     *   <li><b>접속 스키마가 없다</b> — URL 에서 DB 이름을 빠뜨렸다. 마이그레이션은 멀쩡하다
     *   <li><b>메타만 없다</b> — cy-seed 의 {@code ddl/} 로 만든 검증용 셋이 그렇다.
     *       거기에는 {@code BATCH_*} 가 하나도 없어 <b>정상 절차에서 실제로 생긴다</b>
     *   <li><b>데이터 테이블이 없다</b> — 배포 순서를 틀렸다
     * </ul>
     */
    private static String message(String schema, List<String> missing) {
        if (schema == null) {
            return "접속 URL 에 데이터베이스 이름이 없습니다(DATABASE() 가 NULL). "
                    + "jdbc:mysql://host:3306/<db> 형태인지 확인하십시오 — "
                    + "마이그레이션이 아니라 URL 문제입니다.";
        }
        String head = "배치가 보는 스키마(" + schema + ")에 테이블이 없습니다: "
                + String.join(", ", missing) + ". ";
        boolean onlyMeta = missing.stream().allMatch(t -> t.startsWith(META_PREFIX));
        if (onlyMeta) {
            return head + "데이터 테이블은 전부 있으므로 Spring Batch 메타 스키마만 빠진 것입니다 "
                    + "— V2__batch_metadata.sql 을 이 스키마에 부으십시오. "
                    + "cy-seed 의 ddl/ 로 만든 검증용 셋에는 BATCH_* 가 들어 있지 않습니다.";
        }
        return head + "마이그레이션 소유자는 api 입니다 — api 를 먼저 띄워 Flyway 를 끝내십시오. "
                + "검증용 셋이라면 cy-seed 의 ddl/ 을 부은 뒤 V2__batch_metadata.sql 도 함께 부으십시오.";
    }
}
