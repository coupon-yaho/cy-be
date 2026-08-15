// 리플레이가 훑어야 할 발급건 식별자의 양 끝입니다. 구간 청크의 경계가 여기서 나옵니다.
package com.kafkick.core.verification.replay;

/**
 * 양 끝을 한 번에 구해 실행 내내 고정합니다. 재시작할 때 다시 구하면
 * 그 사이 들어온 이력 때문에 범위가 달라져 결정론이 깨집니다.
 */
public record IssuanceIdRange(long min, long max) {

    public IssuanceIdRange {
        if (min > max) {
            throw new IllegalArgumentException(
                    "발급건 구간이 뒤집혔습니다. min=" + min + " max=" + max);
        }
    }
}
