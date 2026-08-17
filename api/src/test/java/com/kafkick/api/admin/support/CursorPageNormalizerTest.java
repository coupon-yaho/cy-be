package com.kafkick.api.admin.support;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 모든 관리자 목록이 공유하는 기본 페이지 크기 정규화 규칙을 검증합니다. */
class CursorPageNormalizerTest {

    /** 요청에서 limit을 생략하면 공통 기본값 50을 사용하는지 검증합니다. */
    @Test
    void defaultsMissingLimitToFifty() {
        assertThat(CursorPageNormalizer.normalizeLimit(null)).isEqualTo(50);
    }

    /** 호출자가 명시한 limit은 기본값으로 덮어쓰지 않는지 검증합니다. */
    @Test
    void preservesExplicitLimit() {
        assertThat(CursorPageNormalizer.normalizeLimit(200)).isEqualTo(200);
    }
}
