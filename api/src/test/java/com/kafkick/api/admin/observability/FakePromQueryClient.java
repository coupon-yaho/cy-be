package com.kafkick.api.admin.observability;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** 질의 문자열에 따라 표본을 돌려주거나 실패하는 시험용 Prometheus 클라이언트입니다. */
final class FakePromQueryClient extends PromQueryClient {

    private final Function<String, List<PromSample>> responder;
    private final List<String> queries = new ArrayList<>();

    FakePromQueryClient(Function<String, List<PromSample>> responder) {
        super(null);
        this.responder = responder;
    }

    /** 모든 질의가 성공하지만 일치하는 시계열이 하나도 없는 상태입니다. */
    static FakePromQueryClient empty() {
        return new FakePromQueryClient(promQl -> List.of());
    }

    /** Prometheus 가 죽은 상태입니다. */
    static FakePromQueryClient down() {
        return new FakePromQueryClient(promQl -> {
            throw new PromQueryException("시험용 장애: " + promQl);
        });
    }

    @Override
    public List<PromSample> query(String promQl) {
        queries.add(promQl);
        return responder.apply(promQl);
    }

    List<String> queries() {
        return List.copyOf(queries);
    }
}
