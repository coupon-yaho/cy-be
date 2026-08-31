package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import com.kafkick.infra.redis.coupon.v2.IssuanceKeys;

/**
 * 설정 템플릿이 적어 둔 V2 키가 <b>어댑터가 실제로 만드는 키</b>와 같은지 본다.
 *
 * <p>다른 테스트들은 양쪽 다 {@link IssuanceKeys} 를 쓰므로 정작 어긋나는 지점 — yml 의
 * 문자열과 코드 — 을 아무도 안 본다. 두 값이 갈리면 리더는 없는 키를 읽어 <b>영구히
 * PENDING</b> 이고, 그 사실은 부하 시험이 끝나고 gap 4축이 안 닫힐 때에야 드러난다.
 *
 * <p>기본값을 V2 로 바꾸지 않는 이유는 <b>V1 측정이 아직 돌기 때문</b>이다. 그래서 계약은
 * "기본값이 V2 다" 가 아니라 <b>"V2 로 켤 때 쓸 문자열이 코드와 같다"</b> 이다.
 */
class DomainGaugeV2KeyTemplateContractTest {

    /** 회차는 아무 값이나 된다 — 보는 것은 자리표시자가 치환된 뒤의 모양이다. */
    private static final long COUPON_ID = 7;

    @Test
    @DisplayName("템플릿의 V2 키가 어댑터의 키와 같다 — 한쪽만 바뀌면 리더가 영구히 PENDING 이다")
    void templateKeysMatchAdapterKeys() {
        IssuanceKeys keys = IssuanceKeys.of(COUPON_ID);

        assertTemplate("DOMAIN_GAUGE_REMAINING_KEY", IssuanceKeys::stock, keys);
        assertTemplate("DOMAIN_GAUGE_ISSUED_EVER_KEY", IssuanceKeys::issuedEver, keys);
        // 회원 집합 = v2 의 issued Hash 다. 이름이 issued 라고 issued_ever 를 가리키면
        // LUA_GAP 의 한쪽 항이 통째로 다른 값이 되고, 그래도 숫자는 나오므로 아무도 못 본다.
        assertTemplate("DOMAIN_GAUGE_MEMBER_EVER_KEY", IssuanceKeys::issued, keys);
    }

    @Test
    @DisplayName("V2 안내에 해시태그가 살아 있다 — 빠지면 Cluster 에서 Lua 가 통째로 CROSSSLOT 이다")
    void templateKeepsHashTag() {
        String template = templateOf("DOMAIN_GAUGE_REMAINING_KEY");

        assertThat(template)
                .contains("{" + DomainGaugeProperties.COUPON_ID_PLACEHOLDER + "}");
    }

    private void assertTemplate(String variable, Function<IssuanceKeys, String> actual, IssuanceKeys keys) {
        assertThat(DomainGaugeProperties.resolve(templateOf(variable), COUPON_ID))
                .as("%s 가 어댑터의 키와 갈라졌다", variable)
                .isEqualTo(actual.apply(keys));
    }

    /**
     * 안내는 주석에 있으므로 YAML 파서가 아니라 원문에서 읽는다. 주석이라도 <b>운영자가 그대로
     * 복사하는 값</b>이라 계약이다.
     */
    private static String templateOf(String variable) {
        String marker = variable + "=";
        return applicationTemplate().lines()
                .map(String::trim)
                .filter(line -> line.startsWith("#") && line.contains(marker))
                .map(line -> line.substring(line.indexOf(marker) + marker.length()).trim())
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "설정 템플릿에 " + variable + " 안내가 없다. V2 로 켤 때 쓸 키를"
                                + " 아무도 못 찾으면 리더는 v1 키를 계속 읽는다."));
    }

    private static String applicationTemplate() {
        try {
            return new String(new ClassPathResource("application.yml.example").getInputStream()
                    .readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
