// 로그인 기능 대신 관리자 API 접근에 사용하는 임시 역할 헤더 규격입니다.
package com.kafkick.api.support;

public final class AdminRequestHeaders {

    public static final String USER_ROLE = "X-User-Role";
    public static final String ADMIN_ROLE = "ADMIN";

    private AdminRequestHeaders() {
    }
}
