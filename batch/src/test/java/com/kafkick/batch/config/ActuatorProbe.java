// 관리 포트에 HTTP 로 물어보는 도구입니다.
package com.kafkick.batch.config;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Boot 4 에는 {@code TestRestTemplate} 이 없다. JDK 클라이언트로 두면 스프링 버전에 안 묶이고,
 * 이 검사가 애초에 보려는 것이 프레임워크가 아니라 <b>HTTP 표면</b>이다.
 *
 * <p>두 actuator 테스트가 같은 것을 각자 갖고 있어 한 곳으로 모았다. 이 저장소는
 * {@code HermeticBoot}·{@code ConfigImportPrecedence} 처럼 공유 도구를 만들어 온 곳이라
 * 여기만 복붙일 이유가 없다.
 */
final class ActuatorProbe {

    private static final Pattern LINK = Pattern.compile("\"([A-Za-z][A-Za-z0-9-]*)\"\\s*:\\s*\\{\"href\"");

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private ActuatorProbe() {
    }

    static HttpResponse<String> get(int port, String path) throws IOException, InterruptedException {
        return CLIENT.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                        .timeout(Duration.ofSeconds(10))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /** 인덱스 본문의 {@code _links} 이름만 뽑는다. 순서는 보장되지 않으므로 집합으로 본다. */
    static Set<String> linkNamesOf(String body) {
        Set<String> names = new LinkedHashSet<>();
        Matcher matcher = LINK.matcher(body);
        while (matcher.find()) {
            names.add(matcher.group(1));
        }
        return names;
    }
}
