package com.kafkick.api.admin.issuance;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery.HistoryPosition;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/** 발급 이력 위치와 외부에 노출할 불투명 HTTP cursor 문자열을 상호 변환합니다. */
@Component
public class IssuanceHistoryCursorCodec {

    private static final int MAX_CURSOR_LENGTH = 256;
    private static final String VERSION = "v1";
    private static final String INVALID_CURSOR_MESSAGE = "유효하지 않은 발급 이력 cursor입니다.";

    /**
     * Core 이력 위치를 padding 없는 Base64 URL cursor로 인코딩합니다.
     *
     * @param position 상태 변경 시각과 이력 ID로 구성된 Keyset 위치
     * @return 같은 위치에 대해 항상 같은 값을 갖는 HTTP cursor
     */
    public String encode(HistoryPosition position) {
        Objects.requireNonNull(position, "position");
        Instant occurredAt = position.occurredAt();
        String payload = VERSION + "|" + occurredAt.getEpochSecond() + "|"
                + occurredAt.getNano() + "|" + position.historyId();
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Base64 URL cursor의 형식과 각 Keyset 값을 검증해 Core 이력 위치로 디코딩합니다.
     *
     * @param cursor 클라이언트가 전달한 불투명 cursor
     * @return 검증을 통과한 상태 변경 시각과 이력 ID
     * @throws BusinessException cursor가 정해진 형식이나 값 범위를 벗어난 경우
     */
    public HistoryPosition decode(String cursor) {
        // 과도한 입력은 Base64 decode보다 먼저 길이에서 차단합니다.
        if (cursor != null && cursor.length() > MAX_CURSOR_LENGTH) {
            throw invalidCursor(null);
        }
        try {
            if (cursor == null || cursor.isBlank() || cursor.indexOf('=') >= 0) {
                throw new IllegalArgumentException("cursor 형식 오류");
            }
            byte[] decoded = Base64.getUrlDecoder().decode(cursor);
            String canonical = Base64.getUrlEncoder().withoutPadding().encodeToString(decoded);
            if (!canonical.equals(cursor)) {
                throw new IllegalArgumentException("cursor 형식 오류");
            }
            String[] segments = new String(decoded, StandardCharsets.UTF_8).split("\\|", -1);
            if (segments.length != 4 || !VERSION.equals(segments[0])) {
                throw new IllegalArgumentException("cursor payload 오류");
            }
            long epochSecond = Long.parseLong(segments[1]);
            int nano = Integer.parseInt(segments[2]);
            long historyId = Long.parseLong(segments[3]);
            if (nano < 0 || nano > 999_999_999 || historyId <= 0L) {
                throw new IllegalArgumentException("cursor 값 범위 오류");
            }
            return new HistoryPosition(Instant.ofEpochSecond(epochSecond, nano), historyId);
        } catch (RuntimeException exception) {
            // 형식과 값의 세부 실패는 외부에서 구분할 수 없는 단일 입력 오류로 통일합니다.
            throw invalidCursor(exception);
        }
    }

    /** cursor 내부 오류를 외부 공통 입력 오류로 변환합니다. */
    private static BusinessException invalidCursor(Throwable cause) {
        if (cause == null) {
            return new BusinessException(CommonErrorCode.INVALID_INPUT, INVALID_CURSOR_MESSAGE);
        }
        return new BusinessException(CommonErrorCode.INVALID_INPUT, INVALID_CURSOR_MESSAGE, cause);
    }
}
