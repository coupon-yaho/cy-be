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
                "/application.yml.example", "/storage.yml.example",
                // ⚠️ **management.yml 이 CY-744 합류로 늘었다.** 그 파일이 노출 목록의 정본이고
                //    (import 문서가 선언 문서를 이긴다) 선언 문서에는 같은 키를 두지 않는다.
                //    대조는 여전히 storage.yml 하나다 — management.yml 은 management.* 만 갖고
                //    선언 문서와 겹치는 키가 없다. 겹치는 날 이 목록에 더한다.
                "classpath:storage.yml", "classpath:management.yml");
    }

    @Test
    @DisplayName("모듈이 받아야 하는 검사 전부 — 앞 문서 충돌과 뒤 문서 순수성")
    void configHoldsUpToEveryPrecedenceRule() {
        precedence().assertEveryRule();
    }

    /**
     * <b>덮어쓸 대상이 없어졌다(CY-744).</b> 예전에는 {@code storage.yml} 이
     * {@code spring.flyway.enabled} 와 {@code spring.datasource.hikari.maximum-pool-size} 를
     * 정의했고, import 로 들어온 그 값이 선언 문서를 이겨서 이 모듈이 적은 값이 조용히
     * 무시됐다. 그래서 덮어쓰기 전용 {@code ---} 문서를 두고 이 검사가 그것을 지켰다.
     *
     * <p><b>main 이 그 두 키를 공유 파일에서 뺐다.</b> {@code storage.yml.example} 이 각각의
     * 자리에 <i>"여기 두지 않는다 — import 로 들어온 문서가 선언 문서를 이긴다"</i> 를 적고
     * 모듈이 선언하도록 넘겼다. 즉 <b>원인이 상류에서 사라졌다.</b>
     *
     * <p>그래서 뒤 문서를 없애고 두 키를 앞 문서로 올렸다. 이 검사는 <b>지우지 않고
     * 뒤집는다</b> — 그 두 키가 다시 공유 파일로 돌아가면 같은 함정이 되살아나므로,
     * <b>겹치지 않는 것</b>을 계약으로 고정한다. {@code assertEveryRule} 이 앞 문서와
     * import 대상의 키 충돌을 이미 보므로, 여기서는 그 규칙이 도는지를 확인한다.
     */
    @Test
    @DisplayName("공유 storage.yml 이 모듈별 값 둘을 다시 가져가지 않는다")
    void sharedConfigDoesNotReclaimPerModuleKeys() {
        precedence().assertNotDefinedByImport(
                "spring.flyway.enabled",
                "spring.datasource.hikari.maximum-pool-size");
    }
}
