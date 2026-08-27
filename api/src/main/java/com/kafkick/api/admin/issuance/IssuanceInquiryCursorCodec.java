package com.kafkick.api.admin.issuance;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.InquiryPosition;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.SourceKind;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/** 발급 문의의 복합 정렬 위치와 외부 불투명 Cursor를 상호 변환합니다. */
@Component
public class IssuanceInquiryCursorCodec {

    private static final int MAX_CURSOR_LENGTH = 256;
    private static final String VERSION = "v1";
    private static final String INVALID_CURSOR_MESSAGE = "유효하지 않은 발급 문의 cursor입니다.";

    /** Core 문의 위치를 padding 없는 정규 Base64 URL Cursor로 인코딩합니다. */
    public String encode(InquiryPosition position) {
        Objects.requireNonNull(position, "position");
        Instant occurredAt = position.occurredAt();
        String payload = VERSION + "|" + occurredAt.getEpochSecond()
                + "|" + occurredAt.getNano()
                + "|" + position.sourceKind().name()
                + "|" + position.sourceId();
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    }

    /** 형식·정규 인코딩·버전·값 범위를 검증해 Core 문의 위치로 디코딩합니다. */
    public InquiryPosition decode(String cursor) {
        // 과도한 입력은 Base64 디코딩과 문자열 할당 전에 차단한다.
        if (cursor != null && cursor.length() > MAX_CURSOR_LENGTH) {
            throw invalidCursor(null);
        }
        try {
            if (cursor == null || cursor.isBlank() || cursor.indexOf('=') >= 0) {
                throw new IllegalArgumentException("cursor 형식 오류");
            }
            byte[] decoded = Base64.getUrlDecoder().decode(cursor);
            // 같은 payload의 다른 Base64 표기를 허용하면 Cursor 문자열의 단일성이 깨진다.
            String canonical = Base64.getUrlEncoder().withoutPadding().encodeToString(decoded);
            if (!canonical.equals(cursor)) {
                throw new IllegalArgumentException("cursor 비정규 인코딩");
            }
            String[] segments = new String(decoded, StandardCharsets.UTF_8).split("\\|", -1);
            if (segments.length != 5 || !VERSION.equals(segments[0])) {
                throw new IllegalArgumentException("cursor payload 오류");
            }
            long epochSecond = Long.parseLong(segments[1]);
            int nano = Integer.parseInt(segments[2]);
            // 같은 숫자 ID가 서로 다른 테이블에 존재할 수 있어 원천 종류도 위치에 포함한다.
            SourceKind sourceKind = SourceKind.valueOf(segments[3]);
            long sourceId = Long.parseLong(segments[4]);
            if (nano < 0 || nano > 999_999_999 || sourceId <= 0L) {
                throw new IllegalArgumentException("cursor 값 범위 오류");
            }
            return new InquiryPosition(
                    Instant.ofEpochSecond(epochSecond, nano), sourceKind, sourceId);
        } catch (RuntimeException exception) {
            throw invalidCursor(exception);
        }
    }

    private static BusinessException invalidCursor(Throwable cause) {
        if (cause == null) {
            return new BusinessException(CommonErrorCode.INVALID_INPUT, INVALID_CURSOR_MESSAGE);
        }
        return new BusinessException(
                CommonErrorCode.INVALID_INPUT, INVALID_CURSOR_MESSAGE, cause);
    }
}
