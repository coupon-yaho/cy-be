package com.kafkick.core.observation.attempt;

/**
 * live 버퍼를 커서 이후로 읽는다. 구현은 {@code infra:redis} 다.
 *
 * <p>쓰는 쪽({@link AttemptLiveSink})과 인터페이스를 나눈 것은 <b>소비하는 모듈이 다르기
 * 때문</b>이다. 쓰기는 {@code infra:mq} 의 컨슈머가, 읽기는 {@code api} 의 관제 컨트롤러가
 * 부른다. 하나로 묶으면 어느 쪽이든 자기가 안 쓰는 절반을 함께 물게 된다.
 */
public interface AttemptLiveReader {

    /**
     * 커서 <b>다음</b> 항목부터 최대 {@code limit} 건을 읽는다.
     *
     * <p>커서가 트림 구간 밖이면 빈 응답이 아니라 <b>현재 첫 항목부터</b> 돌려주고
     * {@link AttemptLivePage#cursorExpired()} 를 세운다. 빈 응답을 주면 화면이 "새 이벤트 없음"
     * 으로 읽어 영원히 멈춘 채로 폴링만 계속한다 — 버퍼는 초당 수천 건이 돌고 있는데.
     *
     * @param afterCursor 마지막으로 소비한 항목의 커서. 최초 조회면 {@code null}
     * @param limit 한 번에 받을 최대 건수
     * @return 커서 이후 한 페이지
     */
    AttemptLivePage readAfter(String afterCursor, int limit);
}
