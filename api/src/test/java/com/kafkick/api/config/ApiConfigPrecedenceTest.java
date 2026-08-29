// api 설정이 storage.yml 에 조용히 덮이는 자리를 막습니다.
package com.kafkick.api.config;

import com.kafkick.storage.db.config.ConfigImportPrecedence;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>batch 에서 터진 사고가 api 에도 그대로 열려 있습니다.</b>
 * {@code api/application.yml} 도 같은 {@code spring.config.import} 를 쓰고, {@code ---} 가 없습니다.
 *
 * <p>지금은 {@code storage.yml} 과 겹치는 키가 <b>0개</b>라 활성 버그가 아닙니다.
 * 위험해지는 것은 api 가 datasource·flyway 키를 처음 적는 순간이고, <b>그때 조용히 죽는 키</b>는 아래가 전부입니다.
 *
 * <pre>
 * spring.datasource.url                       다른 DB 에 붙은 채로 전부 정상 동작한다 — 가장 조용하다
 * spring.datasource.username · password       의도한 최소 권한 계정 대신 storage 계정으로 붙는다
 * spring.flyway.locations                     추가한 로케이션의 마이그레이션이 에러 없이 실행되지 않는다
 * spring.datasource.hikari.maximum-pool-size  max_connections=50 배분이 아무 신호 없이 깨진다
 * </pre>
 *
 * <p><b>이 검사는 키 위치만 봅니다.</b> 결과값 층은 {@link ApiResolvedConfigTest} 가 맡습니다.
 * 다만 지금 api 에는 덮어쓰기 키가 0개라 그 클래스는 <b>덮어쓰기 값을 하나도 단언하지 않습니다</b> —
 * {@code ---} 를 만드는 날 batch 의 {@code ResolvedBatchConfigTest} 처럼 값 단언을 그쪽에 추가해야
 * {@code spring.config.activate.on-profile} 로 문서가 통째로 비활성화되는 틈이 막힙니다.
 *
 * <p>{@code baseline-on-migrate}·{@code validate-on-migrate}·{@code clean-disabled} 는
 * <b>storage.yml 이 지금 값(false·true·true)을 유지하는 한</b> 무시돼도 Flyway 가 기동 중에
 * 예외를 던져 시끄럽습니다. 값이 뒤집히면 얘기가 다릅니다 — storage 가 {@code clean-disabled: false}
 * 가 되면 api 가 적어 둔 {@code true} 가 <b>조용히 죽고 파괴적인 쪽으로 바뀝니다.</b>
 * 그래서 이 검사는 그 셋도 함께 잡습니다. 위 표는 "값이 지금 그대로일 때 조용한 것" 입니다.
 *
 * <p><b>마이그레이션 소유자가 api 라서</b> Flyway 설정을 만질 사람은 이 파일을 엽니다.
 * 그때 이 테스트가 빨개지지 않으면 스키마가 잘못 마이그레이트된 채로 지나갑니다.
 */
class ApiConfigPrecedenceTest {

    /**
     * api 가 실제로 import 하는 목록. 하나라도 늘면 {@code ConfigImportPrecedence.of} 가
     * 그 자리에서 빨개진다 — 새 파일이 선언 문서와 키를 다투는데 아무도 안 보는 상태를 막는다.
     *
     * <p><b>CY-744 합류로 하나에서 다섯으로 늘었다.</b> 예전에는 storage.yml 뿐이었다.
     */
    private static final String[] IMPORTS = {
            "classpath:storage.yml",
            "classpath:management.yml",
            "classpath:observation.yml",
            "classpath:redis.yml",
            "classpath:kafka.yml",
    };

    @Test
    @DisplayName("import 목록이 늘면 빨개진다 — 새 파일이 선언 문서와 다투는 것을 사람이 한 번 본다")
    void importListIsPinned() {
        // ⚠️ **키 겹침(assertEveryRule)까지는 안 본다.** 그 규칙은
        // *"선언 문서는 import 대상과 키를 겹치지 않는다. 덮어쓸 값은 `---` 아래에 둔다"* 인데,
        // 그것은 **batch 설정이 세운 규약**이고 api 는 그 규약을 안 쓴다. 실제로 켜 보니
        // api 의 `---` 문서에 있는 observation.prometheus.* · observation.consistency.* 가
        // 위반으로 잡혔다 — 그 키들은 storage.yml 이 아니라 observation.yml 을 덮는 것이라
        // 정상이다. 남의 모듈에 내 규약을 강제하는 검사가 되므로 그 축은 끈다.
        //
        // ⚠️ 켜려면 대조 대상을 import 다섯 전부로 넓혀야 하는데 redis.yml.example 이
        //    infra/redis 모듈에 있어 api 테스트 클래스패스에 아예 없다. 넓히려면 그 리소스를
        //    먼저 올려야 한다 — 안 올리고 넷만 대조하면 공허하게 통과한다.
        ConfigImportPrecedence.of(getClass(),
                "/application.yml.example", "/storage.yml.example", IMPORTS);
    }
}
