package com.kafkick.api.admin.observability;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** 질의 문자열에 따라 표본을 돌려주거나 실패하는 시험용 Prometheus 질의 대역입니다. */
final class FakePromQuery implements PromQuery {

    private final Function<String, List<PromSample>> responder;
    private final List<String> queries = new ArrayList<>();

    FakePromQuery(Function<String, List<PromSample>> responder) {
        this.responder = responder;
    }

    /** 모든 질의가 성공하지만 일치하는 시계열이 하나도 없는 상태입니다. */
    static FakePromQuery empty() {
        return new FakePromQuery(promQl -> List.of());
    }

    /** Prometheus 가 죽은 상태입니다. */
    static FakePromQuery down() {
        return new FakePromQuery(promQl -> {
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
