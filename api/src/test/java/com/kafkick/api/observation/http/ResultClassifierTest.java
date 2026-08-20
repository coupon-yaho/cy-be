package com.kafkick.api.observation.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.Test;

import com.kafkick.api.observation.http.ResultClassifier.ResultClass;
import com.kafkick.core.observation.Dependency;

class ResultClassifierTest {

    private final ResultClassifier classifier = new ResultClassifier();

    @Test
    void classifiesSixMutuallyExclusiveResults() {
        assertThat(classifier.classify(200, Dependency.NONE)).isEqualTo(ResultClass.SUCCESS);
        assertThat(classifier.classify(201, Dependency.NONE)).isEqualTo(ResultClass.SUCCESS);
        assertThat(classifier.classify(202, Dependency.NONE)).isEqualTo(ResultClass.QUEUE_ACCEPTED);
        assertThat(classifier.classify(403, Dependency.NONE)).isEqualTo(ResultClass.POLICY_REJECT);
        assertThat(classifier.classify(409, Dependency.NONE)).isEqualTo(ResultClass.POLICY_REJECT);
        assertThat(classifier.classify(400, Dependency.NONE)).isEqualTo(ResultClass.CLIENT_INVALID);
    }

    @Test
    void classifiesRemainingClientErrorStatusesAsClientInvalid() {
        assertThat(classifier.classify(401, Dependency.NONE)).isEqualTo(ResultClass.CLIENT_INVALID);
        assertThat(classifier.classify(404, Dependency.NONE)).isEqualTo(ResultClass.CLIENT_INVALID);
        assertThat(classifier.classify(405, Dependency.NONE)).isEqualTo(ResultClass.CLIENT_INVALID);
        assertThat(classifier.classify(422, Dependency.NONE)).isEqualTo(ResultClass.CLIENT_INVALID);
        assertThat(classifier.classify(429, Dependency.NONE)).isEqualTo(ResultClass.CLIENT_INVALID);
    }

    @Test
    void usesDependencyForEverySystemFailureStatus() {
        assertThat(classifier.classify(500, Dependency.REDIS))
                .isEqualTo(ResultClass.DEPENDENCY_FAILURE);
        assertThat(classifier.classify(503, Dependency.REDIS))
                .isEqualTo(ResultClass.DEPENDENCY_FAILURE);
        assertThat(classifier.classify(500, Dependency.NONE))
                .isEqualTo(ResultClass.APPLICATION_FAILURE);
    }

    @Test
    void rejectsMissingDependencyMapping() {
        assertThatNullPointerException()
                .isThrownBy(() -> classifier.classify(500, null))
                .withMessage("dependency");
    }

    @Test
    void rejectsStatusesOutsideTheHttpRange() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> classifier.classify(0, Dependency.NONE));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> classifier.classify(99, Dependency.NONE));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> classifier.classify(600, Dependency.NONE));
    }
}
