package com.kafkick.api.caller;

import java.util.Optional;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.kafkick.api.support.auth.MemberRequestHeaders;

/**
 * {@code X-Member-Id} 헤더로 회원을 가려낸다.
 *
 * <p>헤더 이름은 {@link MemberRequestHeaders#MEMBER_ID} 하나만 쓴다. 관리자 경로와 쿠폰 경로가
 * 서로 다른 이름을 쓰면 새 엔드포인트마다 어느 쪽인지 확인해야 한다.
 *
 * <p>옛 {@code X-User-Id} 는 <b>일부러 받지 않는다.</b> 폴백을 두면 이름이 둘로 되돌아간다 —
 * 하위호환이 필요하면 게이트웨이에서 바꿔 넣는다. {@code CallerFilterTest} 가 이걸 고정한다.
 *
 * <p>{@code members.id} 가 {@code BIGINT} 이므로 {@code long} 으로 파싱한다. 문자열을 정규식으로
 * 거르는 것보다 확실하다 — 파싱을 통과한 값에는 인젝션 문자가 존재할 수 없다.
 */
@Component
public class HeaderCallerResolver implements CallerResolver {

    @Override
    public Optional<Caller> resolve(HttpServletRequest request) {
        String raw = request.getHeader(MemberRequestHeaders.MEMBER_ID);
        if (!StringUtils.hasText(raw)) {
            return Optional.empty();
        }
        try {
            long memberId = Long.parseLong(raw.trim());
            return memberId > 0 ? Optional.of(new Caller(memberId)) : Optional.empty();
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
