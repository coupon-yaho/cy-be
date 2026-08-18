package com.kafkick.api.admin.benchmark.dto;

import java.time.LocalDate;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import com.kafkick.core.observation.EngineVersion;

/**
 * Benchmark 실행 목록의 선택 필터와 과거 방향 cursor를 바인딩하는 변경 가능한 선구축 초안입니다.
 *
 * <p>{@code from}/{@code to}는 선택 기간이며 둘 다 있을 때 역전될 수 없습니다. {@code beforeCursor}는
 * 현재 페이지의 마지막 실행보다 오래된 실행을 가리킵니다. {@code limit}은 지정하는 경우
 * 1~200 범위만 허용됩니다. 엔진 버전은 확정 enum이며 시나리오 코드는 실행 도구와 공유하는 식별 문자열입니다.</p>
 *
 * @param from 조회 시작일; 기간 필터를 사용하지 않으면 null
 * @param to 조회 종료일; 기간 필터를 사용하지 않으면 null
 * @param engineVersion 엔진 버전 필터
 * @param scenarioCode 부하 시나리오 코드 필터
 * @param beforeCursor 현재 페이지보다 오래된 실행을 요청하는 불투명 cursor
 * @param limit 페이지 크기; 선택값, 허용 범위 1~200
 */
public record BenchmarkListQuery(
        LocalDate from,
        LocalDate to,
        EngineVersion engineVersion,
        String scenarioCode,
        String beforeCursor,
        @Min(value = 1, message = "limit은 1 이상이어야 합니다.")
        @Max(value = 200, message = "limit은 200 이하여야 합니다.") Integer limit
) {

    /**
     * 기간의 양 끝이 모두 있을 때 시작일이 종료일보다 늦지 않은지 검증합니다.
     *
     * @return 날짜 한쪽이 없거나 from이 to보다 늦지 않으면 true
     */
    @AssertTrue(message = "from은 to보다 늦을 수 없습니다.")
    public boolean hasChronologicalRange() {
        return from == null || to == null || !from.isAfter(to);
    }
}
