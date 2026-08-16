// 실제 설정 파일 두 벌을 대조해 덮어쓰기가 죽는 자리를 막습니다.
package com.kafkick.batch.config;

import com.kafkick.storage.db.config.ConfigImportPrecedence;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>{@code batch/application.yml} 이 {@code storage.yml} 을 덮어쓰려면 {@code ---} 뒤에 있어야 합니다.</b>
 * 규칙 자체는 {@link ConfigImportPrecedenceRuleTest}, 겹침 판정은 storage 의
 * {@code ConfigImportPrecedenceOverlapTest} 가 증명합니다. 여기서는 <b>우리 파일</b>을 봅니다.
 *
 * <p>이 검사가 필요한 이유는 실패가 조용하기 때문입니다. 키를 잘못 둬도 기동은 되고,
 * 로그에도 아무 말이 없고, 값만 다릅니다. 사람이 눈으로 두 파일을 대조하는 것 말고는
 * 걸릴 방법이 없었습니다 — 실제로 {@code flyway.enabled} 와 {@code maximum-pool-size} 둘 다
 * 그렇게 죽어 있었고 관측 담당자가 Environment 를 찍어 보고서야 드러났습니다.
 *
 * <p>판정기는 {@code storage} 픽스처에 있습니다. {@code api} 가 같은 사고 구조를 가지므로
 * 판정도 한 벌이어야 합니다 — 모듈마다 복사하면 한쪽만 고쳐집니다.
 */
class BatchConfigPrecedenceTest {

    private ConfigImportPrecedence precedence() {
        return ConfigImportPrecedence.of(getClass(),
                "/application.yml.example", "/storage.yml.example", "classpath:storage.yml");
    }

    @Test
    @DisplayName("모듈이 받아야 하는 검사 전부 — 앞 문서 충돌과 뒤 문서 순수성")
    void configHoldsUpToEveryPrecedenceRule() {
        precedence().assertEveryRule();
    }

    @Test
    @DisplayName("storage.yml 을 덮어써야 하는 값은 뒤 문서에 있고 실제로 겹친다")
    void intentionalOverridesLiveAfterTheImport() {
        precedence().assertOverridden(
                "spring.flyway.enabled",
                "spring.datasource.hikari.maximum-pool-size");
    }

}
