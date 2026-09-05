// 배치 실행 파라미터 응답입니다.
package com.kafkick.api.admin.batch.dto;

import java.util.List;

import com.kafkick.core.batch.BatchJobParameter;

/**
 * 한 실행이 <b>무슨 조건으로</b> 돌았는지.
 *
 * <p>실패한 실행을 재현하려면 여기가 시작점이다 — 어떤 {@code asOf} 로, 몇 번째
 * {@code attempt} 로 돌았는지가 없으면 사람이 DB 를 직접 열어야 한다.
 *
 * <p><b>{@code identifying} 이 왜 붙어 있나</b> — 공식 정의로 <b>JobInstance 정체성에
 * 기여한 파라미터</b>다. 이 표시가 있으면 "같은 잡을 같은 조건으로 또 돌린 것" 과
 * "다른 조건" 을 화면에서 가를 수 있고, <b>재시작이 왜 거부되는지</b>도 여기서 읽힌다.
 *
 * <p><b>값을 줄이지 않는다.</b> 실패 원문과 달리 파라미터는 <b>우리가 넣는 값</b>이라
 * 통제가 가능하다 — 대신 <b>"잡 파라미터에 PII 를 넣지 않는다"</b> 가 규약이 된다.
 * 지금 들어오는 것은 {@code asOf}·{@code attempt}·{@code dataset}·{@code scope}·
 * {@code seedRunId}·{@code firedAt} 뿐이다(실측).
 *
 * @param source 이 목록의 원천. 지금은 항상 {@code BATCH_JOB_EXECUTION_PARAMS}
 * @param jobExecutionId 어느 실행의 파라미터인지
 * @param parameters 이름순 파라미터 목록. 빈 목록일 수 있다
 */
public record BatchParameterResponse(String source, long jobExecutionId,
        List<Parameter> parameters) {

    public static final String SOURCE_SPRING_BATCH = "BATCH_JOB_EXECUTION_PARAMS";

    public static BatchParameterResponse of(long jobExecutionId,
            List<BatchJobParameter> parameters) {
        return new BatchParameterResponse(SOURCE_SPRING_BATCH, jobExecutionId,
                parameters.stream().map(Parameter::of).toList());
    }

    /**
     * @param name 파라미터 이름
     * @param type 파라미터 타입. <b>FQCN 이 그대로 온다</b> — 공식 정의가
     *        "fully qualified name of parameter type" 이다
     * @param value 파라미터 값
     * @param identifying JobInstance 정체성에 기여했는지. <b>{@code boolean} 이다</b> —
     *        저장은 {@code 'Y'}/{@code 'N'} 인데 문자로 내보내면 화면이 문자 비교를 하게 되고
     *        대소문자 하나에 조용히 틀린다
     */
    public record Parameter(String name, String type, String value, boolean identifying) {

        static Parameter of(BatchJobParameter parameter) {
            return new Parameter(parameter.name(), parameter.type(),
                    parameter.value(), parameter.identifying());
        }
    }
}
