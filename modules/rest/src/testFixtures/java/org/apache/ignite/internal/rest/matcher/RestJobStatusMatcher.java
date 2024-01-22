/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.rest.matcher;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.time.Instant;
import java.util.UUID;
import org.apache.ignite.internal.rest.api.compute.JobState;
import org.apache.ignite.internal.rest.api.compute.JobStatus;
import org.apache.ignite.internal.testframework.matchers.AnythingMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for {@link JobStatus}.
 */
public class RestJobStatusMatcher extends TypeSafeMatcher<JobStatus> {
    private final Matcher<UUID> idMatcher;
    private final Matcher<JobState> stateMatcher;
    private final Matcher<Instant> createTimeMatcher;
    private final Matcher<Instant> startTimeMatcher;
    private final Matcher<Instant> finishTimeMatcher;

    private RestJobStatusMatcher(
            Matcher<UUID> idMatcher,
            Matcher<JobState> stateMatcher,
            Matcher<Instant> createTimeMatcher,
            Matcher<Instant> startTimeMatcher,
            Matcher<Instant> finishTimeMatcher
    ) {
        this.idMatcher = idMatcher;
        this.stateMatcher = stateMatcher;
        this.createTimeMatcher = createTimeMatcher;
        this.startTimeMatcher = startTimeMatcher;
        this.finishTimeMatcher = finishTimeMatcher;
    }

    // Static method to start building
    public static Builder builder() {
        return new Builder();
    }

    public static RestJobStatusMatcher queued(UUID id) {
        return queued(equalTo(id));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobStatus} has a state of {@link JobState#QUEUED}.
     *
     * @param idMatcher Id matcher.
     * @return Matcher.
     */
    public static RestJobStatusMatcher queued(Matcher<UUID> idMatcher) {
        return builder()
                .withId(idMatcher)
                .withState(JobState.QUEUED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(nullValue(Instant.class))
                .build();
    }

    /**
     * Creates a matcher that matches when the examined {@link JobStatus} has a state of {@link JobState#EXECUTING}.
     *
     * @param id Id.
     * @return Matcher.
     */
    public static RestJobStatusMatcher executing(UUID id) {
        return builder()
                .withId(id)
                .withState(JobState.EXECUTING)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(notNullValue(Instant.class))
                .withFinishTime(nullValue(Instant.class))
                .build();
    }

    /**
     * Creates a matcher that matches when the examined {@link JobStatus} has a state of {@link JobState#FAILED}.
     *
     * @param id Id.
     * @param wasRunning Whether the job was running before it failed.
     * @return Matcher.
     */
    public static RestJobStatusMatcher failed(UUID id, boolean wasRunning) {
        return builder()
                .withId(id)
                .withState(JobState.FAILED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(wasRunning ? notNullValue(Instant.class) : AnythingMatcher.anything())
                .withFinishTime(notNullValue(Instant.class))
                .build();
    }

    /**
     * Creates a matcher that matches when the examined {@link JobStatus} has a state of {@link JobState#COMPLETED}.
     *
     * @param id Id.
     * @return Matcher.
     */
    public static RestJobStatusMatcher completed(UUID id) {
        return builder()
                .withId(id)
                .withState(JobState.COMPLETED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(notNullValue(Instant.class))
                .withFinishTime(notNullValue(Instant.class))
                .build();
    }

    /**
     * Creates a matcher that matches when the examined {@link JobStatus} has a state of {@link JobState#CANCELING}.
     *
     * @param id Id.
     * @param wasRunning Whether the job was running before it was canceled.
     * @return Matcher.
     */
    public static RestJobStatusMatcher canceling(UUID id, boolean wasRunning) {
        return builder()
                .withId(id)
                .withState(JobState.CANCELING)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(wasRunning ? notNullValue(Instant.class) : AnythingMatcher.anything())
                .withFinishTime(notNullValue(Instant.class))
                .build();
    }

    /**
     * Creates a matcher that matches when the examined {@link JobStatus} has a state of {@link JobState#CANCELED}.
     *
     * @param id Id.
     * @param wasRunning Whether the job was running before it was canceled.
     * @return Matcher.
     */
    public static RestJobStatusMatcher canceled(UUID id, boolean wasRunning) {
        return builder()
                .withId(id)
                .withState(JobState.CANCELED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(wasRunning ? notNullValue(Instant.class) : AnythingMatcher.anything())
                .withFinishTime(notNullValue(Instant.class))
                .build();
    }

    @Override
    protected boolean matchesSafely(JobStatus status) {
        return idMatcher.matches(status.id())
                && stateMatcher.matches(status.state())
                && createTimeMatcher.matches(status.createTime())
                && startTimeMatcher.matches(status.startTime())
                && finishTimeMatcher.matches(status.finishTime());
    }

    @Override
    protected void describeMismatchSafely(JobStatus status, Description mismatchDescription) {
        mismatchDescription.appendText("was a JobStatus with id ");
        idMatcher.describeMismatch(status.id(), mismatchDescription);
        mismatchDescription.appendText("state ");
        stateMatcher.describeMismatch(status.state(), mismatchDescription);
        mismatchDescription.appendText(", create time ");
        createTimeMatcher.describeMismatch(status.createTime(), mismatchDescription);
        mismatchDescription.appendText(", start time ");
        startTimeMatcher.describeMismatch(status.startTime(), mismatchDescription);
        mismatchDescription.appendText(" and finish time ");
        finishTimeMatcher.describeMismatch(status.finishTime(), mismatchDescription);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a JobStatus with id ")
                .appendDescriptionOf(idMatcher)
                .appendText(", state ")
                .appendDescriptionOf(stateMatcher)
                .appendText(", create time ")
                .appendDescriptionOf(createTimeMatcher)
                .appendText(", start time ")
                .appendDescriptionOf(startTimeMatcher)
                .appendText(" and finish time ")
                .appendDescriptionOf(finishTimeMatcher);
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Builder for {@link RestJobStatusMatcher}.
     */
    public static class Builder {
        private Matcher<UUID> expectedId = AnythingMatcher.anything();
        private Matcher<JobState> expectedState = AnythingMatcher.anything();
        private Matcher<Instant> expectedCreateTime = AnythingMatcher.anything();
        private Matcher<Instant> expectedStartTime = AnythingMatcher.anything();
        private Matcher<Instant> expectedFinishTime = AnythingMatcher.anything();

        private Builder() {
        }

        private Builder(RestJobStatusMatcher matcher) {
            this.expectedId = matcher.idMatcher;
            this.expectedState = matcher.stateMatcher;
            this.expectedCreateTime = matcher.createTimeMatcher;
            this.expectedStartTime = matcher.startTimeMatcher;
            this.expectedFinishTime = matcher.finishTimeMatcher;
        }

        public Builder withId(UUID id) {
            this.expectedId = equalTo(id);
            return this;
        }

        public Builder withId(Matcher<UUID> id) {
            this.expectedId = id;
            return this;
        }

        public Builder withState(JobState state) {
            this.expectedState = equalTo(state);
            return this;
        }

        public Builder withState(Matcher<JobState> state) {
            this.expectedState = state;
            return this;
        }

        public Builder withCreateTime(Instant createTime) {
            this.expectedCreateTime = equalTo(createTime);
            return this;
        }

        public Builder withCreateTime(Matcher<Instant> createTime) {
            this.expectedCreateTime = createTime;
            return this;
        }

        public Builder withStartTime(Instant startTime) {
            this.expectedStartTime = equalTo(startTime);
            return this;
        }

        public Builder withStartTime(Matcher<Instant> startTime) {
            this.expectedStartTime = startTime;
            return this;
        }

        public Builder withFinishTime(Instant finishTime) {
            this.expectedFinishTime = equalTo(finishTime);
            return this;
        }

        public Builder withFinishTime(Matcher<Instant> finishTime) {
            this.expectedFinishTime = finishTime;
            return this;
        }

        /**
         * Builds a {@link RestJobStatusMatcher}.
         */
        public RestJobStatusMatcher build() {
            return new RestJobStatusMatcher(
                    expectedId,
                    expectedState,
                    expectedCreateTime,
                    expectedStartTime,
                    expectedFinishTime
            );
        }
    }
}
