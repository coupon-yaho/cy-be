package com.kafkick.api.caller;

import java.util.Optional;

import jakarta.servlet.http.HttpServletRequest;

/** 요청에서 회원을 가려낸다. */
public interface CallerResolver {

    Optional<Caller> resolve(HttpServletRequest request);
}
