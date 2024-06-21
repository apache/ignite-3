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
 * Matcher for {@link JobState}.
 */
public class JobStateMatcher extends TypeSafeMatcher<JobState> {
    private final Matcher<JobStatus> statusMatcher;
    private final Matcher<Instant> createTimeMatcher;
    private final Matcher<Instant> startTimeMatcher;
    private final Matcher<Instant> finishTimeMatcher;

    private JobStateMatcher(
            Matcher<JobStatus> statusMatcher,
            Matcher<Instant> createTimeMatcher,
            Matcher<Instant> startTimeMatcher,
            Matcher<Instant> finishTimeMatcher
    ) {
        this.statusMatcher = statusMatcher;
        this.createTimeMatcher = createTimeMatcher;
        this.startTimeMatcher = startTimeMatcher;
        this.finishTimeMatcher = finishTimeMatcher;
    }

    /**
     * Creates a matcher for matching job state with given status.
     *
     * @param status Expected status.
     * @return Matcher for matching job state with given status.
     */
    public static JobStateMatcher jobStateWithStatus(JobStatus status) {
        return jobStateWithStatus(equalTo(status));
    }

    /**
     * Creates a matcher for matching job state with given status.
     *
     * @param statusMatcher Matcher for matching job state status.
     * @return Matcher for matching job state with given status.
     */
    public static JobStateMatcher jobStateWithStatus(Matcher<JobStatus> statusMatcher) {
        return new JobStateMatcher(
                statusMatcher,
                notNullValue(Instant.class),
                anything(),
                anything()
        );
    }

    /**
     * Creates a matcher for matching job state with given status and create time.
     *
     * @param status Expected status.
     * @param createTime Expected create time.
     * @return Matcher for matching job state with given status and create time.
     */
    public static JobStateMatcher jobStateWithStatusAndCreateTime(JobStatus status, Instant createTime) {
        return jobStateWithStatusAndCreateTime(equalTo(status), equalTo(createTime));
    }

    /**
     * Creates a matcher for matching job state with given status and create time.
     *
     * @param statusMatcher Matcher for matching job state status.
     * @param createTimeMatcher Matcher for matching job state create time.
     * @return Matcher for matching job state with given status and create time.
     */
    public static JobStateMatcher jobStateWithStatusAndCreateTime(Matcher<JobStatus> statusMatcher, Matcher<Instant> createTimeMatcher) {
        return new JobStateMatcher(
                statusMatcher,
                createTimeMatcher,
                anything(),
                anything()
        );
    }

    /**
     * Creates a matcher for matching job state with given status, create time, and start time.
     *
     * @param status Expected status.
     * @param createTime Expected create time.
     * @param startTime Expected start time.
     * @return Matcher for matching job state with given status, create time, and start time.
     */
    public static JobStateMatcher jobStateWithStatusAndCreateTimeStartTime(
            JobStatus status,
            Instant createTime,
            Instant startTime
    ) {
        return jobStateWithStatusAndCreateTimeStartTime(equalTo(status), equalTo(createTime), equalTo(startTime));
    }

    /**
     * Creates a matcher for matching job state with given status, create time, and start time.
     *
     * @param statusMatcher Matcher for matching job state status.
     * @param createTimeMatcher Matcher for matching job state create time.
     * @param startTimeMatcher Matcher for matching job state start time.
     * @return Matcher for matching job state with given status, create time, and start time.
     */
    public static JobStateMatcher jobStateWithStatusAndCreateTimeStartTime(
            Matcher<JobStatus> statusMatcher,
            Matcher<Instant> createTimeMatcher,
            Matcher<Instant> startTimeMatcher
    ) {
        return new JobStateMatcher(
                statusMatcher,
                createTimeMatcher,
                startTimeMatcher,
                anything()
        );
    }

    /**
     * Creates a matcher for matching job state with given status, create time, start time, and finish time.
     *
     * @param status Expected status.
     * @param createTime Expected create time.
     * @param startTime Expected start time.
     * @param finishTime Expected finish time.
     * @return Matcher for matching job state with given status, create time, start time, and finish time.
     */
    public static JobStateMatcher jobStateWithStatusAndCreateTimeStartTimeFinishTime(
            JobStatus status,
            Instant createTime,
            Instant startTime,
            Instant finishTime
    ) {
        return jobStateWithStatusAndCreateTimeStartTimeFinishTime(
                equalTo(status),
                equalTo(createTime),
                equalTo(startTime),
                equalTo(finishTime)
        );
    }

    /**
     * Creates a matcher for matching job state with given status, create time, start time, and finish time.
     *
     * @param statusMatcher Matcher for matching job state status.
     * @param createTimeMatcher Matcher for matching job state create time.
     * @param startTimeMatcher Matcher for matching job state start time.
     * @param finishTimeMatcher Matcher for matching job state finish time.
     * @return Matcher for matching job state with given status, create time, start time, and finish time.
     */
    public static JobStateMatcher jobStateWithStatusAndCreateTimeStartTimeFinishTime(
            Matcher<JobStatus> statusMatcher,
            Matcher<Instant> createTimeMatcher,
            Matcher<Instant> startTimeMatcher,
            Matcher<Instant> finishTimeMatcher
    ) {
        return new JobStateMatcher(
                statusMatcher,
                createTimeMatcher,
                startTimeMatcher,
                finishTimeMatcher
        );
    }

    @Override
    protected boolean matchesSafely(JobState status) {
        return statusMatcher.matches(status.status())
                && createTimeMatcher.matches(status.createTime())
                && startTimeMatcher.matches(status.startTime())
                && finishTimeMatcher.matches(status.finishTime());
    }

    @Override
    protected void describeMismatchSafely(JobState state, Description mismatchDescription) {
        mismatchDescription.appendText("status ");
        statusMatcher.describeMismatch(state.status(), mismatchDescription);
        mismatchDescription.appendText(", create time ");
        createTimeMatcher.describeMismatch(state.createTime(), mismatchDescription);
        mismatchDescription.appendText(", start time ");
        startTimeMatcher.describeMismatch(state.startTime(), mismatchDescription);
        mismatchDescription.appendText(" and finish time ");
        finishTimeMatcher.describeMismatch(state.finishTime(), mismatchDescription);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a JobState with status ")
                .appendDescriptionOf(statusMatcher)
                .appendText(", create time ")
                .appendDescriptionOf(createTimeMatcher)
                .appendText(", start time ")
                .appendDescriptionOf(startTimeMatcher)
                .appendText(" and finish time ")
                .appendDescriptionOf(finishTimeMatcher);
    }
}
