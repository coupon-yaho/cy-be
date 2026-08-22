package com.kafkick.core.admin.issuancehistory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class IssuanceCodeMaskerTest {

    private final IssuanceCodeMasker masker = new IssuanceCodeMasker();

    @Test
    void masksTheMiddleEightCharactersOfASixteenCharacterCode() {
        assertThat(masker.mask("ABCD1234EFGH5678"))
                .isEqualTo("ABCD********5678");
    }

    @Test
    void rejectsCodesThatAreNotExactlySixteenCharacters() {
        assertThatThrownBy(() -> masker.mask("ABCD1234EFGH567"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
