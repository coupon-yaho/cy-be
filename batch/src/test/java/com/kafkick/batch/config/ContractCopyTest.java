// 계약 사본이 기록된 리비전 그대로인지 재는 검사입니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>{@code docs/contract.json} 은 읽기 전용 사본이다.</b> 원본은 시드 저장소에 있고
 * {@code docs/11-batch-implementation.md} §계약 원본이 어느 리비전에서 받았는지를 못박는다.
 *
 * <p><b>지키는 그물이 없어서 실제로 갈렸다.</b> 시드에서 전이표를 삼중항으로 바꾸며
 * {@code contract.json} 을 다시 뽑았고 사본도 덮었는데, 문서의 SHA 는 그대로 뒀다 —
 * 사본은 새 내용인데 문서는 옛 리비전을 가리키는 상태로 커밋까지 갔다. 아무도 몰랐다.
 *
 * <p><b>그 어긋남의 대가는 오진이다.</b> 문서가 적어 둔 검증 스크립트는 거기 적힌 SHA 로
 * 원본을 받아 사본과 {@code diff} 하고, <b>차이가 나면 "사본을 손댄 것"</b> 으로 읽으라고
 * 시킨다. 그래서 다음 사람은 <b>멀쩡한 사본을 옛 계약으로 되돌리는</b> 데까지 간다.
 *
 * <p><b>왜 해시로 재나.</b> 리비전이 맞는지는 네트워크 없이 알 수 없다. 대신
 * <i>"이 해시가 이 리비전의 것"</i> 이라는 짝을 여기 적어 두면, 사본이든 문서든 <b>한쪽만</b>
 * 고쳤을 때 반드시 빨개진다. 셋(사본 · 아래 상수 · 문서의 SHA)을 한 묶음으로 만든다.
 *
 * <p><b>{@code SchemaParityTestBase} 의 시드 DDL 사본과 같은 규율이다.</b> 그쪽은
 * README 가, 이쪽은 {@code docs/11} 이 리비전을 진다.
 */
class ContractCopyTest {

    private static final Path CONTRACT = Path.of("../docs/contract.json");

    private static final Path DOC = Path.of("../docs/11-batch-implementation.md");

    /** {@link #CONTRACT_DIGEST} 를 뽑은 시드 저장소 리비전. {@link #DOC} 표와 같아야 한다. */
    private static final String CONTRACT_REVISION = "4307261";

    /** {@link #CONTRACT_REVISION} 시점 {@code contract.json} 의 SHA-256. */
    private static final String CONTRACT_DIGEST =
            "e4cdc1e4444d2ecf1938df41fca9fde6cd3efbd3b8657d4c40ec302909b168b1";

    private static final Pattern ORIGIN = Pattern.compile("@ ([0-9a-f]{7,40})");

    @Test
    @DisplayName("계약 사본이 못박은 리비전과 바이트 동일하다")
    void contractCopyIsPristine() throws IOException {
        assertThat(sha256(CONTRACT))
                .as("사본을 손으로 고치면 배치가 시드와 다른 계약을 따르게 된다. "
                        + "원본(cy-seed/contract.json)을 고치고 거기서 게이트가 성립하는 것을 "
                        + "확인한 뒤 사본을 덮어라 — docs/11 의 SHA 도 함께 고친다")
                .isEqualTo(CONTRACT_DIGEST);
    }

    @Test
    @DisplayName("문서가 적은 리비전과 해시를 뽑은 리비전이 같다")
    void contractCopyPinsOneRevision() throws IOException {
        Matcher matcher = ORIGIN.matcher(Files.readString(DOC, StandardCharsets.UTF_8));

        assertThat(matcher.find())
                .as("docs/11 이 '@ <sha>' 꼴로 원본 리비전을 적어야 갱신 절차가 성립한다")
                .isTrue();

        assertThat(matcher.group(1))
                .as("사본을 갱신할 때 세 곳을 함께 고친다 — docs/contract.json · "
                        + "CONTRACT_REVISION/CONTRACT_DIGEST · docs/11 의 SHA")
                .isEqualTo(CONTRACT_REVISION);
    }

    private static String sha256(Path path) throws IOException {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path));
            StringBuilder hex = new StringBuilder(64);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16))
                        .append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 이 없는 JVM 은 없다", e);
        }
    }
}
