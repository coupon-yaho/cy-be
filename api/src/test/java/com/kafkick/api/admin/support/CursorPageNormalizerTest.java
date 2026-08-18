package com.kafkick.api.admin.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.benchmark.dto.BenchmarkListQuery;
import com.kafkick.api.admin.issuance.dto.IssuanceHistoryQuery;
import com.kafkick.api.admin.issuance.dto.IssuanceInquiryQuery;
import com.kafkick.api.admin.support.config.AdminPaginationProperties;

/** 모든 관리자 목록이 공유하는 기본 페이지 크기 정규화 규칙을 검증합니다. */
class CursorPageNormalizerTest {

    /** 요청에서 limit을 생략하면 설정으로 주입한 기본값을 사용하는지 검증합니다. */
    @Test
    void defaultsMissingLimitToConfiguredValue() {
        CursorPageNormalizer normalizer = new CursorPageNormalizer(new AdminPaginationProperties(37));

        assertThat(normalizer.normalizeLimit(null)).isEqualTo(37);
    }

    /** 호출자가 명시한 limit은 기본값으로 덮어쓰지 않는지 검증합니다. */
    @Test
    void preservesExplicitLimit() {
        CursorPageNormalizer normalizer = new CursorPageNormalizer(new AdminPaginationProperties(37));

        assertThat(normalizer.normalizeLimit(200)).isEqualTo(200);
    }

    /** 설정 기본값도 HTTP 계약의 1~200 범위를 벗어날 수 없습니다. */
    @Test
    void rejectsConfiguredLimitOutsideHttpContract() {
        assertThatThrownBy(() -> new AdminPaginationProperties(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminPaginationProperties(201))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Query는 설정 적용 전 누락 limit을 보존하고 복사 시에만 정규화합니다. */
    @Test
    void queryRecordsApplyNormalizedLimitExplicitly() {
        BenchmarkListQuery benchmark = new BenchmarkListQuery(
                LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 2), null, null, null, null);
        IssuanceHistoryQuery history = new IssuanceHistoryQuery(null, null, null, null, null, null);
        IssuanceInquiryQuery inquiry = new IssuanceInquiryQuery(1L, null, null, null, null, null);

        assertThat(benchmark.limit()).isNull();
        assertThat(history.limit()).isNull();
        assertThat(inquiry.limit()).isNull();
        assertThat(benchmark.withLimit(37).limit()).isEqualTo(37);
        assertThat(history.withLimit(37).limit()).isEqualTo(37);
        assertThat(inquiry.withLimit(37).limit()).isEqualTo(37);
    }
}
