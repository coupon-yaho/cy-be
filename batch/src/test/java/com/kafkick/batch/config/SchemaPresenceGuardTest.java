// 스키마가 없는 DB 에 붙은 배치가 기동에 성공하지 않는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.VerificationRuleRepository;
import com.kafkick.core.verification.exception.VerificationErrorCode;

/**
 * <b>batch 는 빈 DB 에서도 기동이 그냥 성공한다.</b> {@code @Entity} 가 없어
 * {@code ddl-auto: validate} 가 공허하게 통과하고, 마이그레이션 소유자는 {@code api} 라
 * {@code flyway.enabled:false} 다. compose 의 {@code depends_on: service_healthy} 도
 * {@code mysqladmin ping} 까지만 보장한다 — 어느 층도 "스키마가 있나" 를 안 본다.
 *
 * <p><b>CY-359 가 그 침묵의 대가를 올렸다.</b> {@link VerificationMetrics} 가 자기 데이터셋을
 * {@code hasCleanOnlyConstraints()} 로 정하는데 그것은 인덱스 하나를 묻는 {@code EXISTS} 라
 * <b>테이블이 하나도 없으면 예외 없이 {@code false}</b> 를 준다. 그러면 빈 DB 에 붙은
 * 프로세스가 {@code dataset="CORRUPT"} 라벨로 지표를 내보낸다 — 관제에 나가는 라벨이 틀린다.
 *
 * <p><b>여기서 재는 것은 판정과 메시지뿐이다.</b> 목록이 비면 통과하는가, 하나라도 없으면
 * 죽는가, 그리고 <b>원인마다 다른 조치를 말하는가</b>.
 *
 * <p><b>배선은 여기서 못 잰다.</b> 인스턴스를 손으로 만들어 부르므로 {@code @Component} 를
 * 떼거나 스캔 밖 패키지로 옮겨도 이 클래스는 초록이다. 나머지 배치 테스트도 증명이 안 된다 —
 * 전부 스키마를 갖춘 컨테이너 위에서 <b>통과 경로만</b> 밟기 때문에 빈이 없어도 초록이다.
 * 그 축은 {@code VerificationMetricExposureTest.schemaGuardRunsBeforeAnyJob} 이 진다.
 *
 * <p>가드를 넣자마자 {@code ActuatorExposureTest} 둘이 <b>빈 스키마 위에서 떠 있었다</b>는
 * 사실이 드러났다 — 그것이 이 가드가 막으려는 상태의 실제 사례다.
 */
class SchemaPresenceGuardTest {

    /**
     * <b>이 축이 없으면 가드는 "아무것도 안 하는 빈"</b>이어도 초록이다. 모든 배치 테스트가
     * 스키마를 갖춘 컨테이너 위에서 도니, 통과 경로만으로는 가드가 일하는지 알 수 없다.
     */
    @Test
    @DisplayName("테이블이 하나라도 없으면 기동을 죽인다")
    void refusesToStartWhenACoreTableIsMissing() {
        SchemaPresenceGuard guard = new SchemaPresenceGuard(missing(List.of("verification_runs")));

        assertThatThrownBy(() -> guard.run(null))
                .isInstanceOf(BusinessException.class)
                .as("이름이 메시지에 없으면 사람이 어느 테이블인지 다시 조회해야 한다")
                .hasMessageContaining("verification_runs")
                .as("배포 순서 문제라는 것과 조치가 메시지에 있어야 한다 — 스택트레이스가 "
                        + "JDBC 계층이면 '검증 배치가 깨졌다' 로 읽힌다")
                .hasMessageContaining("api");

        assertThat(catchCode(guard))
                .as("에러코드가 다른 것으로 바뀌면 이 가드를 가리키는 문서가 전부 죽는다")
                .isEqualTo(VerificationErrorCode.SCHEMA_NOT_MIGRATED);
    }

    /**
     * 목록 <b>전체</b>가 메시지에 실려야 한다. 하나만 싣고 죽으면 사람이 고친 뒤 다시 뜨고
     * 다음 것에서 또 죽는다 — 빈 DB 는 일곱이 한꺼번에 없는 상태다.
     */
    @Test
    @DisplayName("빈 스키마면 없는 테이블을 전부 알려 준다")
    void namesEveryMissingTable() {
        List<String> all = List.of(
                "issuances", "issuance_histories", "verification_runs", "coupon_stocks",
                "BATCH_JOB_INSTANCE", "BATCH_JOB_EXECUTION", "BATCH_STEP_EXECUTION");

        assertThatThrownBy(() -> new SchemaPresenceGuard(missing(all)).run(null))
                .isInstanceOf(BusinessException.class)
                .satisfies(e -> assertThat(e.getMessage()).contains(all));
    }

    /**
     * <b>같은 증상이 세 원인을 갖는다.</b> 조치가 각각 달라서, 한 문장으로 접으면 사람이
     * 엉뚱한 것을 재배포하며 시간을 쓴다.
     *
     * <p>메타만 빈 상태는 <b>정상 절차에서 실제로 생긴다</b> — 검증용 셋은 cy-seed 의
     * {@code ddl/} 로 만들어지는데 거기에 {@code BATCH_*} 가 하나도 없다. 그때 필요한 조치는
     * "api 를 먼저 띄워라" 가 아니라 {@code V2__batch_metadata.sql} 을 붓는 것이다.
     */
    @Test
    @DisplayName("메타만 없으면 V2 를 부으라고 한다 — api 재배포가 아니다")
    void tellsToApplyBatchMetadataWhenOnlyMetaIsMissing() {
        assertThatThrownBy(() -> new SchemaPresenceGuard(
                missing(List.of("BATCH_JOB_INSTANCE", "BATCH_JOB_EXECUTION"))).run(null))
                .hasMessageContaining("V2__batch_metadata.sql")
                .as("데이터 테이블은 멀쩡한데 api 를 다시 띄우게 하면 안 된다")
                .hasMessageNotContaining("api 를 먼저 띄워");
    }

    /**
     * <b>URL 에서 DB 이름을 빠뜨린 배포는 마이그레이션 문제가 아니다.</b>
     * {@code table_schema = DATABASE()} 가 {@code NULL} 비교라 UNKNOWN 이 되어 넷이 아니라
     * <b>전부</b> 없다고 나오는데, 그때 api 를 아무리 재배포해도 안 낫는다.
     */
    @Test
    @DisplayName("접속 스키마가 없으면 URL 문제라고 말한다")
    void tellsAboutTheUrlWhenThereIsNoSchema() {
        assertThatThrownBy(() -> new SchemaPresenceGuard(
                stub(null, List.of("issuances", "verification_runs"))).run(null))
                .hasMessageContaining("DATABASE() 가 NULL")
                .as("마이그레이션을 다시 하라고 시키면 원인에서 멀어진다")
                .hasMessageNotContaining("Flyway");
    }

    @Test
    @DisplayName("메시지에 접속 스키마 이름이 있다 — 어디를 보고 있는지가 첫 질문이다")
    void namesTheSchemaItIsLookingAt() {
        assertThatThrownBy(() -> new SchemaPresenceGuard(
                stub("coupon_clean", List.of("issuances"))).run(null))
                .hasMessageContaining("coupon_clean");
    }

    /**
     * <b>테이블이 다 있다고 컬럼도 있는 것은 아니다.</b> cy-seed {@code 1f217b5} 이전에 만든
     * 검증용 셋이 정확히 그 모양이다 — 그대로 띄우면 기동은 통과하고 되읽기만 매 주기
     * 조용히 실패한다. 조치도 다르다: 재생성 말고는 답이 없다.
     */
    @Test
    @DisplayName("테이블은 다 있고 컬럼만 없으면 재생성하라고 한다")
    void tellsToRebuildTheDatasetWhenOnlyAColumnIsMissing() {
        assertThatThrownBy(() -> new SchemaPresenceGuard(
                stub("coupon_clean", List.of(), List.of("verification_runs.origin"))).run(null))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("verification_runs.origin")
                .as("테이블이 멀쩡한데 배포 순서를 의심하게 하면 안 된다")
                .hasMessageNotContaining("api 를 먼저 띄워")
                .hasMessageContaining("다시 만드십시오");
    }

    @Test
    @DisplayName("전부 있으면 통과한다")
    void passesWhenNothingIsMissing() {
        assertThatCode(() -> new SchemaPresenceGuard(missing(List.of())).run(null))
                .doesNotThrowAnyException();
    }

    private static VerificationErrorCode catchCode(SchemaPresenceGuard guard) {
        try {
            guard.run(null);
            throw new AssertionError("죽어야 한다");
        } catch (BusinessException e) {
            return (VerificationErrorCode) e.getErrorCode();
        }
    }

    /**
     * 가드가 보는 두 축만 답하는 최소 스텁이다. 나머지 메서드를 부르면
     * {@code UnsupportedOperationException} 이라, 가드가 조용히 다른 축을 보게 되면 드러난다.
     */
    private static VerificationRuleRepository missing(List<String> tables) {
        return stub("app", tables, List.of());
    }

    private static VerificationRuleRepository stub(String schema, List<String> tables) {
        return stub(schema, tables, List.of());
    }

    private static VerificationRuleRepository stub(String schema, List<String> tables,
            List<String> columns) {
        return (VerificationRuleRepository) java.lang.reflect.Proxy.newProxyInstance(
                VerificationRuleRepository.class.getClassLoader(),
                new Class<?>[] {VerificationRuleRepository.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "missingCoreTables" -> tables;
                    case "missingCriticalColumns" -> columns;
                    case "currentSchema" -> schema;
                    default -> throw new UnsupportedOperationException(
                            "가드가 예상 밖의 축을 본다: " + method.getName());
                });
    }
}
