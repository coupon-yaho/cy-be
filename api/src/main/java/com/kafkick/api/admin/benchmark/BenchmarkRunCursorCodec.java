package com.kafkick.api.admin.benchmark;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.benchmark.BenchmarkRunPosition;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/** Benchmark 복합 Keyset 위치와 padding 없는 Base64 URL cursor를 상호 변환합니다. */
@Component
public class BenchmarkRunCursorCodec {
    /** Core 위치를 HTTP cursor로 인코딩합니다. */
    public String encode(BenchmarkRunPosition position) {
        Objects.requireNonNull(position, "position");
        String value = "v1|" + position.startedAt().getEpochSecond() + "|" + position.startedAt().getNano() + "|" + position.benchmarkRunId();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
    /**
     * HTTP cursor를 검증한 Core Keyset 위치로 디코딩합니다.
     *
     * @throws BusinessException cursor가 입력 길이, 정규 Base64 URL 형식, 버전 또는
     *                           Keyset 값 계약을 위반한 경우(INVALID_INPUT, HTTP 400)
     */
    public BenchmarkRunPosition decode(String cursor) {
        try {
            if (cursor == null || cursor.isBlank() || cursor.length() > 256 || cursor.indexOf('=') >= 0) throw new IllegalArgumentException();
            byte[] bytes = Base64.getUrlDecoder().decode(cursor);
            if (!Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(cursor)) throw new IllegalArgumentException();
            String[] parts = new String(bytes, StandardCharsets.UTF_8).split("\\|", -1);
            if (parts.length != 4 || !"v1".equals(parts[0])) throw new IllegalArgumentException();
            return new BenchmarkRunPosition(Instant.ofEpochSecond(Long.parseLong(parts[1]), Integer.parseInt(parts[2])), Long.parseLong(parts[3]));
        } catch (RuntimeException exception) { throw new BusinessException(CommonErrorCode.INVALID_INPUT, "유효하지 않은 Benchmark cursor입니다.", exception); }
    }
}
