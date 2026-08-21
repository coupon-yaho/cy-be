// 잡 실행에서 실패 메시지를 뽑습니다.
package com.kafkick.batch.job;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.job.JobExecution;

/**
 * 같은 코드가 여러 테스트에 복붙돼 있었다. 파싱 방식을 바꿔야 할 때 — 이를테면 원인 체인이
 * 아니라 억제된 예외까지 봐야 할 때 — 한쪽만 고치면 나머지가 조용히 옛 방식으로 남는다.
 */
final class JobFailures {

    private JobFailures() {
    }

    /** 원인 체인을 끝까지 편다. 배치는 예외를 여러 겹으로 감싸 던진다. */
    static List<String> messagesOf(JobExecution execution) {
        List<String> messages = new ArrayList<>();
        for (Throwable failure : execution.getAllFailureExceptions()) {
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                messages.add(String.valueOf(cause.getMessage()));
            }
        }
        return messages;
    }
}
