package com.kafkick.storage.db.coupon.codec;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import com.kafkick.core.coupon.exception.IdempotencyPersistenceException;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;

abstract class JacksonIdempotencyResultCodec<R>
        implements IdempotencyResultCodec<R> {

    private final ObjectMapper objectMapper;
    private final Class<R> resultType;
    private final String operationName;

    protected JacksonIdempotencyResultCodec(
            ObjectMapper objectMapper,
            Class<R> resultType,
            String operationName
    ) {
        this.objectMapper = objectMapper;
        this.resultType = resultType;
        this.operationName = operationName;
    }

    @Override
    public final String write(R result) {
        try {
            return objectMapper.writeValueAsString(result);
        } catch (JacksonException exception) {
            throw new IdempotencyPersistenceException(
                    operationName + " 결과 직렬화에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public final R read(String responseBody) {
        try {
            return objectMapper.readValue(responseBody, resultType);
        } catch (JacksonException exception) {
            throw new IdempotencyPersistenceException(
                    "저장된 " + operationName + " 결과를 읽지 못했습니다.",
                    exception
            );
        }
    }
}
