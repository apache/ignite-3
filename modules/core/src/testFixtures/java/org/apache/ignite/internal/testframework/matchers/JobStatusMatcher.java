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

package org.apache.ignite.internal.testframework.matchers;

import static org.apache.ignite.internal.testframework.matchers.AnythingMatcher.anything;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import java.time.Instant;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for {@link JobStatus}.
 */
public class JobStatusMatcher extends TypeSafeMatcher<JobStatus> {
    private final Matcher<JobState> stateMatcher;
    private final Matcher<Instant> createTimeMatcher;
    private final Matcher<Instant> startTimeMatcher;
    private final Matcher<Instant> finishTimeMatcher;

    private JobStatusMatcher(
            Matcher<JobState> stateMatcher,
            Matcher<Instant> createTimeMatcher,
            Matcher<Instant> startTimeMatcher,
            Matcher<Instant> finishTimeMatcher
    ) {
        this.stateMatcher = stateMatcher;
        this.createTimeMatcher = createTimeMatcher;
        this.startTimeMatcher = startTimeMatcher;
        this.finishTimeMatcher = finishTimeMatcher;
    }

    /**
     * Creates a matcher for matching job status with given state.
     *
     * @param state Expected state.
     * @return Matcher for matching job status with given state.
     */
    public static JobStatusMatcher jobStatusWithState(JobState state) {
        return jobStatusWithState(equalTo(state));
    }

    /**
     * Creates a matcher for matching job status with given state.
     *
     * @param stateMatcher Matcher for matching job status state.
     * @return Matcher for matching job status with given state.
     */
    public static JobStatusMatcher jobStatusWithState(Matcher<JobState> stateMatcher) {
        return new JobStatusMatcher(
                stateMatcher,
                notNullValue(Instant.class),
                anything(),
                anything()
        );
    }

    /**
     * Creates a matcher for matching job status with given state and create time.
     *
     * @param state Expected state.
     * @param createTime Expected create time.
     * @return Matcher for matching job status with given state and create time.
     */
    public static JobStatusMatcher jobStatusWithStateAndCreateTime(JobState state, Instant createTime) {
        return jobStatusWithStateAndCreateTime(equalTo(state), equalTo(createTime));
    }

    /**
     * Creates a matcher for matching job status with given state and create time.
     *
     * @param stateMatcher Matcher for matching job status state.
     * @param createTimeMatcher Matcher for matching job status create time.
     * @return Matcher for matching job status with given state and create time.
     */
    public static JobStatusMatcher jobStatusWithStateAndCreateTime(Matcher<JobState> stateMatcher, Matcher<Instant> createTimeMatcher) {
        return new JobStatusMatcher(
                stateMatcher,
                createTimeMatcher,
                anything(),
                anything()
        );
    }

    /**
     * Creates a matcher for matching job status with given state, create time, and start time.
     *
     * @param state Expected state.
     * @param createTime Expected create time.
     * @param startTime Expected start time.
     * @return Matcher for matching job status with given state, create time, and start time.
     */
    public static JobStatusMatcher jobStatusWithStateAndCreateTimeStartTime(
            JobState state,
            Instant createTime,
            Instant startTime
    ) {
        return jobStatusWithStateAndCreateTimeStartTime(equalTo(state), equalTo(createTime), equalTo(startTime));
    }

    /**
     * Creates a matcher for matching job status with given state, create time, and start time.
     *
     * @param stateMatcher Matcher for matching job status state.
     * @param createTimeMatcher Matcher for matching job status create time.
     * @param startTimeMatcher Matcher for matching job status start time.
     * @return Matcher for matching job status with given state, create time, and start time.
     */
    public static JobStatusMatcher jobStatusWithStateAndCreateTimeStartTime(
            Matcher<JobState> stateMatcher,
            Matcher<Instant> createTimeMatcher,
            Matcher<Instant> startTimeMatcher
    ) {
        return new JobStatusMatcher(
                stateMatcher,
                createTimeMatcher,
                startTimeMatcher,
                anything()
        );
    }

    /**
     * Creates a matcher for matching job status with given state, create time, start time, and finish time.
     *
     * @param state Expected state.
     * @param createTime Expected create time.
     * @param startTime Expected start time.
     * @param finishTime Expected finish time.
     * @return Matcher for matching job status with given state, create time, start time, and finish time.
     */
    public static JobStatusMatcher jobStatusWithStateAndCreateTimeStartTimeFinishTime(
            JobState state,
            Instant createTime,
            Instant startTime,
            Instant finishTime
    ) {
        return jobStatusWithStateAndCreateTimeStartTimeFinishTime(
                equalTo(state),
                equalTo(createTime),
                equalTo(startTime),
                equalTo(finishTime)
        );
    }

    /**
     * Creates a matcher for matching job status with given state, create time, start time, and finish time.
     *
     * @param stateMatcher Matcher for matching job status state.
     * @param createTimeMatcher Matcher for matching job status create time.
     * @param startTimeMatcher Matcher for matching job status start time.
     * @param finishTimeMatcher Matcher for matching job status finish time.
     * @return Matcher for matching job status with given state, create time, start time, and finish time.
     */
    public static JobStatusMatcher jobStatusWithStateAndCreateTimeStartTimeFinishTime(
            Matcher<JobState> stateMatcher,
            Matcher<Instant> createTimeMatcher,
            Matcher<Instant> startTimeMatcher,
            Matcher<Instant> finishTimeMatcher
    ) {
        return new JobStatusMatcher(
                stateMatcher,
                createTimeMatcher,
                startTimeMatcher,
                finishTimeMatcher
        );
    }

    @Override
    protected boolean matchesSafely(JobStatus status) {
        return stateMatcher.matches(status.state())
                && createTimeMatcher.matches(status.createTime())
                && startTimeMatcher.matches(status.startTime())
                && finishTimeMatcher.matches(status.finishTime());
    }

    @Override
    protected void describeMismatchSafely(JobStatus status, Description mismatchDescription) {
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
        description.appendText("a JobStatus with state ")
                .appendDescriptionOf(stateMatcher)
                .appendText(", create time ")
                .appendDescriptionOf(createTimeMatcher)
                .appendText(", start time ")
                .appendDescriptionOf(startTimeMatcher)
                .appendText(" and finish time ")
                .appendDescriptionOf(finishTimeMatcher);
    }
}

