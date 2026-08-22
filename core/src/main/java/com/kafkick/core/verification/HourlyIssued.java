// 요일·시각 한 칸의 발급 수입니다.
package com.kafkick.core.verification;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <b>요일 표기는 세 글자다.</b> {@code hourly_stats.day_of_week} 가 {@code varchar(3)} 이고
 * 시드가 {@code MON TUE WED THU FRI SAT SUN} 을 쓴다({@code cy-seed/seedgen/config.py}).
 * {@code DayOfWeek.name()} 은 {@code MONDAY} 라 그대로 쓰면 안 된다.
 *
 * <p><b>변환은 자바가 아니라 SQL 이 한다.</b> MySQL {@code WEEKDAY()} 는 0 = 월요일이라
 * 파이썬 {@code weekday()} 와 같고, {@code ELT(WEEKDAY(x) + 1, 'MON', …)} 이 시드의
 * {@code DOW[at.weekday()]} 와 <b>글자 단위로 대응</b>한다. {@code DAYNAME()} 은
 * {@code lc_time_names} 세션 변수에 의존해 쓰지 않는다 — 서버 로케일이 바뀌면 값이 갈린다.
 */
public record HourlyIssued(String dayOfWeek, int hour, int issuedTotal) {

    /** 시드의 {@code DOW} 배열과 같은 순서. 파이썬 {@code weekday()} 는 0 = 월요일이다. */
    private static final List<String> DAYS =
            List.of("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN");

    public HourlyIssued {
        if (!DAYS.contains(dayOfWeek)) {
            throw new IllegalArgumentException(
                    "요일은 " + DAYS + " 중 하나여야 합니다. 값=" + dayOfWeek);
        }
        if (hour < 0 || hour > 23) {
            throw new IllegalArgumentException("시각은 0~23 입니다. 값=" + hour);
        }
        if (issuedTotal < 0) {
            throw new IllegalArgumentException("발급 수는 음수일 수 없습니다. 값=" + issuedTotal);
        }
    }

    /**
     * <b>빈 칸을 0 으로 채워 168행을 만든다.</b> 없는 행과 0인 행은 다르다 — 빼면 대시보드가
     * "그 시각에 데이터가 없다" 와 "그 시각에 0건이다" 를 구분할 수 없다.
     *
     * <p>MySQL 에는 {@code generate_series} 가 없어 SQL 로 채우려면 {@code UNION ALL} 168줄이
     * 된다. 168행은 자바에서 만드는 편이 읽기 쉽고 비용도 없다.
     */
    public static List<HourlyIssued> fillAll(List<HourlyIssued> measured) {
        Map<String, HourlyIssued> found = measured.stream().collect(
                Collectors.toMap(h -> h.dayOfWeek() + h.hour(), Function.identity()));

        List<HourlyIssued> all = new ArrayList<>(DAYS.size() * 24);
        for (String day : DAYS) {
            for (int hour = 0; hour < 24; hour++) {
                all.add(found.getOrDefault(day + hour, new HourlyIssued(day, hour, 0)));
            }
        }

        return all;
    }
}
