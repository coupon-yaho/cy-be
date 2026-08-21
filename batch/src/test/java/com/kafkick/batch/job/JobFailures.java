// 잡 실행에서 실패 메시지를 뽑습니다.
package com.kafkick.batch.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.springframework.batch.core.job.JobExecution;

import com.kafkick.core.support.exception.BusinessException;

/**
 * 같은 코드가 여러 테스트에 복붙돼 있었다. 파싱 방식을 바꿔야 할 때 — 이를테면 원인 체인이
 * 아니라 억제된 예외까지 봐야 할 때 — 한쪽만 고치면 나머지가 조용히 옛 방식으로 남는다.
 */
final class JobFailures {

    private JobFailures() {
    }

    /**
     * 원인 체인을 끝까지 편다. 배치는 예외를 여러 겹으로 감싸 던진다.
     *
     * <p><b>이미 지나온 예외를 만나면 멈춘다.</b> {@code initCause} 로 A→B→A 가 만들어지면
     * 순진한 순회는 끝나지 않는다 — <b>실패한 테스트보다 진단이 어렵다.</b> 테스트가 죽는
     * 대신 CI 가 타임아웃까지 매달리고, 로그에는 마지막 줄만 남아 원인이 이 헬퍼라는 것이
     * 안 보인다.
     *
     * <p>같음이 아니라 <b>동일성</b>으로 본다({@link IdentityHashMap}). {@code equals} 를
     * 재정의한 예외가 체인에 있으면 서로 다른 인스턴스가 하나로 접혀 메시지가 빠진다.
     */
    static List<String> messagesOf(JobExecution execution) {
        List<String> messages = new ArrayList<>();
        for (Throwable failure : execution.getAllFailureExceptions()) {
            Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Throwable cause = failure; cause != null && seen.add(cause);
                    cause = cause.getCause()) {
                messages.add(String.valueOf(cause.getMessage()));
            }
        }
        return messages;
    }

    /**
     * 원인 체인에서 만난 {@link BusinessException} 의 <b>에러 코드</b>만 모은다.
     *
     * <p><b>메시지 문자열은 계약이 아니다.</b> 문구를 다듬으면 테스트가 깨지고, 반대로
     * 에러 코드가 바뀌어도 문구가 그대로면 테스트는 통과한다 — 둘 다 틀렸다.
     * 잡이 무엇 때문에 죽었는지의 계약은 {@code ExpirationErrorCode} 이고, 알림·관제가
     * 무엇을 갈라 보는지도 그 코드다.
     */
    static List<String> errorCodesOf(JobExecution execution) {
        List<String> codes = new ArrayList<>();
        for (Throwable failure : execution.getAllFailureExceptions()) {
            Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Throwable cause = failure; cause != null && seen.add(cause);
                    cause = cause.getCause()) {
                if (cause instanceof BusinessException business) {
                    codes.add(business.getErrorCode().getCode());
                }
            }
        }
        return codes;
    }
}
