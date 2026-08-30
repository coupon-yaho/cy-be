// 배치가 보는 스키마에 핵심 테이블이 있는지 기동 시점에 확인합니다.
package com.kafkick.batch.config;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.VerificationRuleRepository;
import com.kafkick.core.verification.exception.VerificationErrorCode;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

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
 * <p><b>축이 셋이다.</b> 테이블 · 핵심 컬럼 · 성능 인덱스. 앞 둘은 없으면 잡이 SQL
 * 에러로 죽지만 <b>셋째는 없어도 기동과 동작이 통과한다</b> — 조용히 느려질 뿐이라
 * 늦게, 그리고 원인을 안 가리키며 드러난다(CY-686 이 더했다).
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

    /**
     * <b>메타 마이그레이션 셋 전체.</b> {@link #run} 이 테이블·컬럼·인덱스 세 축을 다 보므로
     * (CY-686 이 셋째를 더했다), 안내가 인덱스 둘까지 함께 말해야 한 번에 절차가 끝난다 —
     * 테이블만 부으면 그 자리에서 셋째 축이 다시 거절한다.
     */
    private static final String META_MIGRATIONS =
            "V11__batch_metadata.sql · V2026082513__ix_batch_job_execution_lookup.sql · V2026082514__ix_batch_job_execution_history.sql";

    /**
     * <b>인덱스마다 파일과 증상이 다르다.</b> 한 문장으로 접으면 사람이 엉뚱한 것을 본다 —
     * 이 클래스가 {@link #message} 에서 이미 못 박은 규율이다. {@code docs/14} 가 두 증상을
     * 갈라 놨다: 앞엣것은 게이지가 {@code NaN} 이 되고, 뒤엣것은 <b>게이지는 멀쩡한 채</b>
     * {@code CleanupRunningTooLong} 으로만 뜬다.
     */
    private static final Map<String, String> INDEX_REMEDY = Map.of(
            "IX_JOB_EXEC_STATUS_END",
            "V2026082513__ix_batch_job_execution_lookup.sql — 없으면 되읽기가 "
                    + "STATUS·END_TIME 을 전체 스캔해 데드라인을 넘기고 게이지가 NaN 이 됩니다",
            "IX_JOB_EXEC_CREATE_TIME",
            "V2026082514__ix_batch_job_execution_history.sql — 없으면 정리 잡이 "
                    + "CREATE_TIME 을 매 청크 전체 스캔합니다. 게이지는 멀쩡하고 "
                    + "CleanupRunningTooLong 으로만 뜹니다");

    /**
     * <b>인덱스 이름만 떼어 처방을 찾는다.</b> 포트가 주는 값은
     * {@code TABLE.INDEX(COL1,COL2)} 인데, <b>컬럼 튜플까지 키로 쓰면 결합이 부러진다</b> —
     * 상수는 {@code storage} 에 있고 이 맵은 {@code batch} 에 있어 컴파일러가 안 묶어 준다
     * ({@code batch → storage} 가 {@code runtimeOnly} 다). 인덱스 정의를 고치는 날 한쪽만
     * 바뀌면 운영자가 조치 대신 빈 문자열을 받는다. 이름은 그 변경에 안 흔들린다.
     */
    private static String remedyFor(String qualified) {
        int dot = qualified.indexOf('.');
        int paren = qualified.indexOf('(');
        String name = qualified.substring(dot + 1, paren < 0 ? qualified.length() : paren);
        // **폴백에 파일 이름을 안 싣는다.** 셋을 다 대면 없는 인덱스 하나를 두고 파일
        // 전부를 읽으라는 말이 되어, 인덱스별로 갈라 놓은 이유가 사라진다.
        return INDEX_REMEDY.getOrDefault(name,
                "이 인덱스의 처방이 등록돼 있지 않습니다 — INDEX_REMEDY 를 확인하십시오");
    }

    /** 거절을 끄는 손잡이. 기본은 켬 — 끄는 법은 거절 메시지가 직접 말한다. */
    static final String REQUIRE_INDEXES = "batch.schema-guard.require-batch-indexes";

    private final VerificationRuleRepository rules;

    private final boolean requireIndexes;

    private final MeterRegistry registry;

    /** 없는 인덱스 수. 게이지가 이 값을 읽는다 — 러너가 한 번만 도므로 필드에 남긴다. */
    private volatile int missingIndexCount;

    public SchemaPresenceGuard(VerificationRuleRepository rules,
            @Value("${" + REQUIRE_INDEXES + ":true}") boolean requireIndexes,
            MeterRegistry registry) {
        this.rules = rules;
        this.requireIndexes = requireIndexes;
        this.registry = registry;
    }

    /**
     * <b>끈 상태를 지표로 낸다.</b> 이 저장소의 알림은 전부 Prometheus 지표 위에 서 있고
     * Loki·promtail 이 없다 — 즉 <b>로그는 감시 수단이 아니다</b>({@code CleanupJobConfig} 의
     * yield 주석이 같은 판단을 적어 뒀다). 거절을 끄고 띄운 상태가 ERROR 로그 한 줄로만
     * 남으면, 며칠 뒤 {@code CleanupRunningTooLong} 을 보는 사람이 그 줄에 못 닿는다.
     * 형제 {@code cy_coupon_round_scheduling_enabled} 와 같은 모양이다.
     */
    private void publish(int missing) {
        this.missingIndexCount = missing;
        if (registry == null) {
            return;
        }
        Gauge.builder("cy_batch_schema_index_missing", this, self -> self.missingIndexCount)
                .description("배치 메타 성능 인덱스 중 없는 것의 수 — 0 이 정상")
                .register(registry);
        Gauge.builder("cy_batch_schema_index_enforcement", () -> requireIndexes ? 1 : 0)
                .description("인덱스 축 거절이 켜져 있는가 — 1 켜짐 · 0 꺼짐")
                .register(registry);
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
        // **셋째 축이다(CY-686).** 위 둘은 없으면 잡이 SQL 에러로 죽지만, 인덱스는
        // 없어도 기동과 동작이 통과한다 — 되읽기가 데드라인을 넘겨 게이지가 NaN 이 되거나
        // 정리 잡이 매 청크 전체 스캔을 하는 것으로만 드러나고, 둘 다 원인을 안 가리킨다.
        //
        // ⚠️ **거절에 탈출구를 둔다.** 앞 두 축과 달리 이 축은 **정확성이 아니라 성능**이다.
        //    그런데 여기서 못 뜨면 같은 프로세스의 CouponRoundScheduler 도 안 돌아
        //    open_at 이 지난 회차가 SCHEDULED 로 남고 **발급 문이 안 열린다** — 만료·정리도
        //    함께 선다. 방어의 대가가 방어 대상보다 크면 안 되므로, 기본은 거절하되
        //    푸는 법을 메시지가 직접 말한다.
        List<String> missingIndexes = rules.missingCriticalIndexes();
        publish(missingIndexes.size());
        if (!missingIndexes.isEmpty()) {
            String remedies = missingIndexes.stream()
                    .map(name -> name + " → " + remedyFor(name))
                    .collect(Collectors.joining(" / "));
            if (!requireIndexes) {
                log.error("배치 메타 인덱스가 없습니다 — 거절은 꺼져 있습니다({}=false). {}",
                        REQUIRE_INDEXES, remedies);
                // **여기서 끝낸다.** 아래 "전부 있습니다" 를 찍으면 바로 윗줄을 부정한다 —
                // 기동 로그를 grep 으로 훑는 사람이 인덱스 축을 후보에서 뺀다.
                log.warn("스키마 확인 완료 — 테이블·컬럼은 전부 있고 인덱스 검사는 "
                        + "꺼져 있습니다. schema={}", schema);
                return;
            } else {
                throw new BusinessException(VerificationErrorCode.SCHEMA_NOT_MIGRATED,
                        "배치가 보는 스키마(" + schema + ")에 인덱스가 없습니다: " + remedies
                                + ". 테이블과 컬럼은 전부 있으므로 메타 마이그레이션 셋 중 "
                                + "인덱스만 빠진 것입니다 — 그 파일을 이 스키마에 부으십시오. "
                                + "이름은 맞는데 걸린다면 컬럼 구성이 다른 것입니다(가드가 "
                                + "선두 컬럼까지 봅니다). 지금 당장 띄워야 하면 환경변수 "
                                + "SCHEMA_GUARD_REQUIRE_BATCH_INDEXES=false (또는 실행 인자 "
                                + "--" + REQUIRE_INDEXES + "=false) 로 끌 수 있습니다 — "
                                + "그 경우 관제 지표가 흐려지고 정리가 느려집니다.");
            }
        }
        log.info("스키마 확인 완료 — 배치 핵심 테이블·컬럼·인덱스가 전부 있습니다. schema={}",
                schema);
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
                    + "— 배치 메타 마이그레이션 셋(" + META_MIGRATIONS + ")을 이 스키마에 "
                    + "부으십시오. 인덱스 둘도 함께 부으십시오 — 테이블만 부으면 이 가드의 "
                    + "셋째 축이 그 자리에서 다시 거절합니다(CY-686). "
                    + "cy-seed 의 ddl/ 로 만든 검증용 셋에는 BATCH_* 가 들어 있지 않습니다.";
        }
        return head + "마이그레이션 소유자는 api 입니다 — api 를 먼저 띄워 Flyway 를 끝내십시오. "
                + "검증용 셋이라면 cy-seed 의 ddl/ 을 부은 뒤 배치 메타 마이그레이션 셋("
                + META_MIGRATIONS + ")도 함께 부으십시오.";
    }
}
