// 배치 실행 파라미터 한 건입니다.
package com.kafkick.core.batch;

/**
 * 잡 실행 파라미터 한 건. {@code BATCH_JOB_EXECUTION_PARAMS} 한 행에 대응한다.
 *
 * <h2>왜 필요한가</h2>
 *
 * <p>실패한 실행을 <b>재현하려면 무슨 조건으로 돌았는지</b>가 시작점이다. 이 저장소의 잡은
 * {@code asOf}·{@code attempt}·{@code dataset}·{@code scope}·{@code seedRunId}·
 * {@code firedAt} 을 받는데(실측), 관제가 그것을 안 주면 사람이 DB 를 직접 열어야 한다.
 *
 * <h2>{@code identifying} 이 왜 값진가</h2>
 *
 * <p>공식 문서의 정의 그대로 <b>"JobInstance 정체성에 기여한 파라미터"</b> 다.
 * 이 표시가 있으면 <b>"같은 잡을 같은 조건으로 또 돌린 것"</b> 과 <b>"다른 조건"</b> 을
 * 화면에서 가를 수 있다 — 재시작이 왜 거부되는지도 여기서 읽힌다.
 *
 * <p><b>{@code CHAR(1)} 을 그대로 안 나른다.</b> 저장은 {@code 'Y'}/{@code 'N'} 인데
 * 문자열로 내보내면 화면이 문자 비교를 하게 되고, 대소문자 하나에 <b>조용히 틀린다.</b>
 * 경계에서 {@code boolean} 으로 바꾼다.
 *
 * <h2>{@code value} 를 그대로 싣는다 — 그 한계를 적어 둔다</h2>
 *
 * <p>지금 들어오는 값은 전부 시각·회차·이름이라 문제가 없다(실측). 다만 이 관제는 범용이라
 * <b>앞으로 무엇이 파라미터로 들어올지 이 코드가 정하지 못한다.</b> 실패 원문
 * ({@link FailureSummary})과 달리 여기는 <b>우리가 넣는 값</b>이므로 통제가 가능하다 —
 * 그래서 줄이지 않고, 대신 <b>"잡 파라미터에 PII 를 넣지 않는다"</b> 를 규약으로 둔다.
 * 그 규약이 깨지면 이 화면이 그대로 새는 자리라는 뜻이다.
 *
 * @param name 파라미터 이름
 * @param type 파라미터 타입. <b>FQCN 이 그대로 온다</b>({@code java.lang.String} 등) —
 *        공식 문서가 "fully qualified name of parameter type" 이라고 정의한다
 * @param value 파라미터 값. <b>줄이지 않는다</b> — 위 설명
 * @param identifying JobInstance 정체성에 기여했는지
 */
public record BatchJobParameter(String name, String type, String value, boolean identifying) {
}
