// 관리자 API 에 공유 비밀 헤더 관문을 답니다.
package com.kafkick.batch.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.ZoneOffset;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.core.support.response.ErrorResponse;
import com.kafkick.core.support.response.ResponseEnvelope;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.verification.exception.VerificationErrorCode;

/**
 * <b>관리자 API 에 최소한의 관문을 단다.</b> {@code /api/v1/admin/**} 에는 트리거만 있는 것이
 * 아니라 {@code stop} · {@code recover} · {@code abandon} 이 있다 — 포트에 닿는 누구나
 * <b>도는 잡을 죽일 수 있다</b>. {@code docs/11} §11 은 그 위험을 <b>포트 미노출</b> 하나로
 * 막기로 했는데, 관제 화면을 붙이면서 그 전제가 흔들린다.
 *
 * <p><b>{@code docs/11} §11 과 모순되지 않는다.</b> 거기서 거부한 것은 <b>서명 없는 역할
 * 클레임</b>({@code hasRole})이다 — 클라이언트가 <i>주장</i>하는 값이라 방어가 아니라 장식이다.
 * 이 관문은 <b>주장이 아니라 소지</b>를 묻는다: 값을 <b>가진</b> 쪽만 통과한다. 그래서
 * JWT·세션·Spring Security 를 안 들이고도 <i>"아무나"</i> 를 <i>"토큰을 받은 사람"</i> 으로 좁힌다.
 *
 * <p><b>이것으로 충분하다고 말하지 않는다.</b> TLS 가 없으면 평문으로 오가고, 값 하나라
 * 유출되면 회수 수단이 재기동뿐이다. <b>포트 미노출이 여전히 1차 방어선</b>이고 이것은 그 뒤에
 * 서는 2차다 — 바인딩을 넓히는 결정을 이 관문이 대신해 주지 않는다.
 *
 * <p><b>비교는 상수 시간으로 한다.</b> {@code equals} 는 앞에서부터 다른 바이트를 만나면 바로
 * 끝나서, 응답 시간 차이로 한 글자씩 맞혀 볼 수 있다. 길이가 다르면 어차피 다르므로 길이는
 * 가려도 손해가 없다.
 *
 * <p><b>실패는 로그에 남기되 토큰은 절대 안 찍는다.</b> 이 저장소는 로그를 사람이 붙여 넣는
 * 일이 잦고({@code docs/14} 의 절차들), 한 번 찍히면 회수할 수 없다.
 *
 * <p><b>프리플라이트({@code OPTIONS})는 통과시킨다.</b> 브라우저는 프리플라이트에 사용자
 * 헤더를 안 싣는다 — 여기서 401 을 주면 <b>본 요청이 아예 안 나가고</b> 관제 버튼이
 * "CORS 오류" 로만 보인다. 프리플라이트에는 본문도 부수효과도 없다.
 */
public class AdminTokenFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(AdminTokenFilter.class);

    /** 관제가 실을 헤더 이름. {@code Authorization} 을 쓰지 않는다 — 그 이름은 영역 ③의 토큰 규약 몫이다. */
    public static final String HEADER = "X-Batch-Admin-Token";

    private final byte[] expected;
    private final ObjectMapper objectMapper;
    private final TimeProvider timeProvider;

    AdminTokenFilter(String token, ObjectMapper objectMapper, TimeProvider timeProvider) {
        this.expected = token.getBytes(StandardCharsets.UTF_8);
        this.objectMapper = objectMapper;
        this.timeProvider = timeProvider;
    }

    /**
     * <b>경로 판정을 여기서 하지 않는다.</b> 스코프는 {@code AdminTokenConfig} 의
     * {@code addUrlPatterns("/api/v1/admin/*")} 가 <b>컨테이너 레벨</b>에서 잡는다 —
     * 톰캣의 매퍼는 <b>디코딩·정규화된</b> 경로로 매칭한다.
     *
     * <p>⚠️ <b>여기서 접두사를 한 번 더 보는 것이 오히려 구멍이었다.</b>
     * {@code getRequestURI()} 는 서블릿 스펙상 <b>디코딩되지 않은 원문</b>이라,
     * {@code /api/v1/%61dmin/...} 처럼 한 글자만 퍼센트 인코딩하면
     * {@code startsWith("/api/v1/admin/")} 이 거짓이 되어 <b>관문을 건너뛴다</b>.
     * 그런데 톰캣은 그 요청을 이 필터에 매핑했고 스프링은 컨트롤러까지 라우팅한다 —
     * <b>완전한 인증 우회</b>다. 실측에서 {@code %61dmin} · {@code adm%69n} ·
     * {@code admi%6e} · {@code %61pi} 넷이 전부 <b>200</b> 을 냈다(CY-742 보안 리뷰).
     *
     * <p>컨텍스트 경로를 벗기는 것으로도 안 낫는다 — 원문을 보는 한 인코딩 우회는 그대로다.
     * <b>두 판정 기준을 겹치지 않게 하는 것</b>이 답이라, 컨테이너 하나에 맡긴다.
     * {@code AdminTokenBypassTest} 가 두 계열을 다 못 박는다.
     */
    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return HttpMethod.OPTIONS.matches(request.getMethod());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
            FilterChain chain) throws ServletException, IOException {
        String presented = request.getHeader(HEADER);
        if (presented != null && MessageDigest.isEqual(
                presented.getBytes(StandardCharsets.UTF_8), expected)) {
            chain.doFilter(request, response);
            return;
        }

        // 있는데 틀린 것과 아예 없는 것을 로그에서 가른다 — 전자는 설정 실수, 후자는 남의 요청이다.
        log.warn("관리자 API 토큰이 {}. method={} path={}",
                presented == null ? "없습니다" : "맞지 않습니다",
                request.getMethod(), request.getRequestURI());

        response.setStatus(VerificationErrorCode.ADMIN_TOKEN_REQUIRED.getStatus());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        // **어느 쪽인지 응답으로 알려 주지 않는다.** 틀린 값과 없는 값을 가르면 그 자체가
        // 힌트가 된다. 형제인 BatchApiExceptionHandler 도 같은 이유로 상세를 로그에만 남긴다.
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, HEADER);

        // **같은 표면에서 봉투 모양이 갈리면 안 된다.** 이 필터는 DispatcherServlet 앞에서
        // 응답하므로 BatchApiExceptionHandler 를 안 지나지만, 관제가 파싱하는 JSON 은
        // 같아야 한다 — requestId·timestamp 가 빠지면 그 필드를 늘 있다고 믿는 클라이언트가
        // 이 401 에서만 깨진다. requestId 가 null 인 것도 형제와 같다(batch 에는 그것을
        // MDC 에 심는 필터가 없다).
        objectMapper.writeValue(response.getWriter(), ResponseEnvelope.fail(
                ErrorResponse.of(VerificationErrorCode.ADMIN_TOKEN_REQUIRED, null,
                        timeProvider.now().toInstant(ZoneOffset.UTC))));
    }
}
