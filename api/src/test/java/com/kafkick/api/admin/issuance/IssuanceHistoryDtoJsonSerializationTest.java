package com.kafkick.api.admin.issuance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.admin.issuance.dto.IssuanceHistoryPageResponse;
import com.kafkick.api.admin.issuance.dto.IssuanceHistoryPageResponse.IssuanceHistoryItem;
import com.kafkick.api.admin.issuance.dto.IssuanceHistoryPageResponse.IssuanceHistorySummary;
import com.kafkick.api.admin.issuance.dto.IssuanceHistoryQuery;
import com.kafkick.api.admin.support.AdminJsonTest;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery.HistoryPosition;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistoryItem;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistorySummary;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;

/** 발급 이력 요청의 Core 변환과 응답 JSON 경계를 검증합니다. */
@AdminJsonTest
class IssuanceHistoryDtoJsonSerializationTest {

    private final ObjectMapper objectMapper;
    private final IssuanceHistoryCursorCodec cursorCodec = new IssuanceHistoryCursorCodec();

    /** 실제 API Jackson 설정이 적용된 직렬화기를 주입받습니다. */
    @Autowired
    IssuanceHistoryDtoJsonSerializationTest(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /** 한국 날짜 양 끝과 빈 cursor, 기본 limit을 정확한 Core 조회 조건으로 변환하는지 검증합니다. */
    @Test
    @DisplayName("조회 날짜를 KST 포함·제외 경계로 바꾸고 기본 limit 50과 빈 cursor를 적용한다")
    void convertsKstDateBoundariesBlankCursorAndDefaultLimit() {
        IssuanceHistoryQuery query = new IssuanceHistoryQuery(
                101L, "  ", null,
                LocalDate.parse("2026-08-23"), LocalDate.parse("2026-08-23"),
                IssuanceEventType.USE);

        AdminIssuanceHistoryQuery coreQuery = query.toCoreQuery(cursorCodec);

        assertThat(coreQuery.couponId()).isEqualTo(101L);
        assertThat(coreQuery.fromInclusive()).isEqualTo(Instant.parse("2026-08-22T15:00:00Z"));
        assertThat(coreQuery.toExclusive()).isEqualTo(Instant.parse("2026-08-23T15:00:00Z"));
        assertThat(coreQuery.eventType()).isEqualTo(IssuanceEventType.USE);
        assertThat(coreQuery.before()).isNull();
        assertThat(coreQuery.limit()).isEqualTo(AdminIssuanceHistoryQuery.DEFAULT_LIMIT);
    }

    /** 불투명 HTTP cursor와 명시 limit을 Core Keyset 위치로 전달하는지 검증합니다. */
    @Test
    @DisplayName("유효한 beforeCursor를 Core 이력 위치로 decode하고 명시 limit을 유지한다")
    void decodesBeforeCursorAndKeepsExplicitLimit() {
        HistoryPosition position = new HistoryPosition(Instant.parse("2026-08-23T01:02:03.123456789Z"), 42L);
        IssuanceHistoryQuery query = new IssuanceHistoryQuery(
                null, cursorCodec.encode(position), 17, null, null, null);

        AdminIssuanceHistoryQuery coreQuery = query.toCoreQuery(cursorCodec);

        assertThat(coreQuery.before()).isEqualTo(position);
        assertThat(coreQuery.limit()).isEqualTo(17);
    }

    /** Core의 마스킹 항목·다음 위치·5종 요약을 공개 JSON 계약으로 옮기는지 검증합니다. */
    @Test
    @DisplayName("Core 결과를 cursor와 5종 요약을 포함한 불변 JSON 응답으로 변환한다")
    void convertsCoreResultToImmutableJsonContract() throws Exception {
        Instant occurredAt = Instant.parse("2026-08-23T01:02:03Z");
        HistoryPosition nextBefore = new HistoryPosition(occurredAt, 9L);
        AdminIssuanceHistoryResult result = new AdminIssuanceHistoryResult(
                List.of(new HistoryItem(
                        5001L, "A101-****-0001", 101L, null, IssuanceStatus.ISSUED,
                        IssuanceEventType.ISSUE, occurredAt)),
                nextBefore,
                true,
                new HistorySummary(5L, 1L, 1L, 1L, 1L, 1L));

        IssuanceHistoryPageResponse response = IssuanceHistoryPageResponse.from(result, cursorCodec);
        String json = objectMapper.writeValueAsString(response);

        assertThat(response.items()).containsExactly(new IssuanceHistoryItem(
                5001L, "A101-****-0001", 101L, null, IssuanceStatus.ISSUED,
                IssuanceEventType.ISSUE, occurredAt));
        assertThat(response.nextBeforeCursor()).isEqualTo(cursorCodec.encode(nextBefore));
        assertThat(response.summary()).isEqualTo(new IssuanceHistorySummary(5L, 1L, 1L, 1L, 1L, 1L));
        assertThatThrownBy(() -> response.items().add(response.items().getFirst()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(json)
                .contains("\"items\"", "\"nextBeforeCursor\"", "\"hasOlder\":true", "\"summary\"")
                .contains("\"eventType\":\"ISSUE\"", "\"toStatus\":\"ISSUED\"")
                .contains("\"issuanceCodeMasked\":\"A101-****-0001\"")
                .doesNotContain("\"fromStatus\"");
    }

    /** 마지막 페이지에서는 Core 위치가 없어야 하고 HTTP cursor도 null인지 검증합니다. */
    @Test
    @DisplayName("더 과거 이력이 없으면 nextBeforeCursor를 null로 반환한다")
    void omitsNextCursorOnLastPage() {
        AdminIssuanceHistoryResult result = new AdminIssuanceHistoryResult(
                List.of(), null, false, new HistorySummary(0L, 0L, 0L, 0L, 0L, 0L));

        IssuanceHistoryPageResponse response = IssuanceHistoryPageResponse.from(result, cursorCodec);

        assertThat(response.hasOlder()).isFalse();
        assertThat(response.nextBeforeCursor()).isNull();
        assertThat(response.items()).isEmpty();
    }

    /** 직접 생성할 때도 mutable 목록을 복사하고 목록·요약 null을 거부하는지 검증합니다. */
    @Test
    @DisplayName("응답 record가 목록을 방어 복사하고 목록과 요약 null을 거부한다")
    void responseRecordDefensivelyCopiesItemsAndRejectsNullContracts() {
        ArrayList<IssuanceHistoryItem> mutableItems = new ArrayList<>();
        IssuanceHistorySummary emptySummary = new IssuanceHistorySummary(0L, 0L, 0L, 0L, 0L, 0L);
        IssuanceHistoryPageResponse response = new IssuanceHistoryPageResponse(
                mutableItems, null, false, emptySummary);

        mutableItems.add(new IssuanceHistoryItem(
                1L, "MASKED", 1L, null, IssuanceStatus.ISSUED,
                IssuanceEventType.ISSUE, Instant.EPOCH));

        assertThat(response.items()).isEmpty();
        assertThatThrownBy(() -> new IssuanceHistoryPageResponse(null, null, false, emptySummary))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new IssuanceHistoryPageResponse(List.of(), null, false, null))
                .isInstanceOf(NullPointerException.class);
    }
}
