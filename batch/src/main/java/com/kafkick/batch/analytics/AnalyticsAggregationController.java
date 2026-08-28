package com.kafkick.batch.analytics;

import java.time.Instant;
import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 집계를 <b>지금 한 번</b> 돌리는 내부 트리거.
 *
 * <p>크론은 1시간이라 데이터를 넣고 화면을 확인하려면 최대 그만큼 기다려야 한다. 장애 뒤 따라잡기도
 * 다음 회차를 기다려야 한다. 이 입구는 그 대기를 없앤다.
 *
 * <h2>노출 — 무엇이 막고 무엇이 안 막는가</h2>
 *
 * <p>{@code batch} 는 compose 가 <b>포트를 발행하지 않는다.</b> 그래서 도커 네트워크 밖에서는 닿지
 * 않고, 관제 API 가 내부망으로 부르는 형태다. 같은 경로에 이미 두 개가 같은 조건으로 서 있다
 * ({@code /internal/v1/benchmarks/consistency/final} · {@code /preflight}).
 *
 * <p>⚠️ <b>"관리자만" 은 아니다.</b> 이 저장소에는 관리자를 판정하는 인증이 없다 —
 * {@code Caller} 는 스스로 <i>"인증 결과가 아니라 사용자 구분 수단"</i> 이라고 적어 두었고 Spring
 * Security 도 없다. 그래서 헤더로 역할을 주장하게 만들면 <b>막는 것 없이 막는 것처럼 보이기만</b>
 * 한다. 지금 실제로 막는 것은 <b>포트 미발행</b> 하나뿐이고, 관리자 노출은 앞단(관제 API)이 맡는다.
 * TODO(후속 티켓): 내부 트리거 세 개에 같은 인증 규칙을 세운다. 이 엔드포인트만 따로 잠그면
 * 한 저장소에 두 규칙이 생기므로 셋을 함께 봐야 한다.
 *
 * <p>기준 시각은 <b>지금</b>으로 박힌다. 과거 시각을 받지 않는다 — 이미 센 지점보다 이른 기준으로
 * 다시 세면 값이 작아지고 되돌아오지 않기 때문이다(그 방어는 {@code AnalyticsAggregationRunner}
 * 가 갖고 있다).
 */
/*
 * ⚠️ 조건을 **클래스에** 단다. @RestController 는 컴포넌트 스캔에 걸리므로, 조건이 없으면 관측을
 *    끈 배포에서도 이 빈이 만들어지고 Runner 를 못 찾아 <b>기동이 죽는다</b>(실측 — 관측을 끈
 *    컨텍스트 테스트가 그 자리에서 깨졌다). 조건부 빈을 무는 소비자는 같은 조건을 져야 한다.
 *
 *    스케줄러와 달리 조건이 하나뿐이라 설정 파일이 아니라 여기 둔다. 동결 스위치는 안 문다 —
 *    그것은 저절로 도는 쓰기를 멈추는 것이고, 이 입구는 사람이 일부러 부르는 것이다.
 */
@RestController
@RequestMapping("/internal/v1/analytics")
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class AnalyticsAggregationController {

    private final AnalyticsAggregationRunner runner;

    public AnalyticsAggregationController(AnalyticsAggregationRunner runner) {
        this.runner = runner;
    }

    @PostMapping("/aggregate")
    public AggregateResponse aggregate() {
        AnalyticsAggregationResult result = runner.runOnce();
        return new AggregateResponse(
                result.runId(),
                result.asOf(),
                result.succeeded(),
                result.writtenRows().entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .toList(),
                result.failedAxes().keySet().stream().map(Enum::name).toList());
    }

    /** 실패 <b>사유</b>는 싣지 않는다 — 회차 이력과 로그가 갖고 있고, 이 응답은 내부 확인용이다. */
    public record AggregateResponse(
            long runId,
            Instant asOf,
            boolean succeeded,
            List<String> writtenRows,
            List<String> failedAxes
    ) {
        public AggregateResponse {
            writtenRows = List.copyOf(writtenRows);
            failedAxes = List.copyOf(failedAxes);
        }
    }
}
