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
import org.apache.ignite.compute.TaskState;
import org.apache.ignite.compute.TaskStatus;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for {@link JobState}.
 */
public class TaskStateMatcher extends TypeSafeMatcher<TaskState> {
    private final Matcher<TaskStatus> statusMatcher;
    private final Matcher<Instant> createTimeMatcher;
    private final Matcher<Instant> startTimeMatcher;
    private final Matcher<Instant> finishTimeMatcher;


    private TaskStateMatcher(
            Matcher<TaskStatus> statusMatcher,
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
    public static TaskStateMatcher taskStateWithStatus(TaskStatus status) {
        return taskStateWithStatus(equalTo(status));
    }

    /**
     * Creates a matcher for matching job state with given status.
     *
     * @param statusMatcher Matcher for matching job state status.
     * @return Matcher for matching job state with given status.
     */
    public static TaskStateMatcher taskStateWithStatus(Matcher<TaskStatus> statusMatcher) {
        return new TaskStateMatcher(
                statusMatcher,
                notNullValue(Instant.class),
                anything(),
                anything()
        );
    }

    /**
     * Creates a matcher for matching task state with given status, create time, start time, and finish time.
     *
     * @param statusMatcher Matcher for matching task state status.
     * @param createTimeMatcher Matcher for matching task state create time.
     * @param startTimeMatcher Matcher for matching task state start time.
     * @param finishTimeMatcher Matcher for matching task state finish time.
     * @return Matcher for matching task state with given status, create time, start time, and finish time.
     */
    public static TaskStateMatcher taskStateWithStatusAndCreateTimeStartTimeFinishTime(
            Matcher<TaskStatus> statusMatcher,
            Matcher<Instant> createTimeMatcher,
            Matcher<Instant> startTimeMatcher,
            Matcher<Instant> finishTimeMatcher
    ) {
        return new TaskStateMatcher(
                statusMatcher,
                createTimeMatcher,
                startTimeMatcher,
                finishTimeMatcher
        );
    }

    @Override
    protected boolean matchesSafely(TaskState status) {
        return statusMatcher.matches(status.status())
                && createTimeMatcher.matches(status.createTime())
                && startTimeMatcher.matches(status.startTime())
                && finishTimeMatcher.matches(status.finishTime());
    }

    @Override
    protected void describeMismatchSafely(TaskState state, Description mismatchDescription) {
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
        description.appendText("a TaskState with status ")
                .appendDescriptionOf(statusMatcher)
                .appendText(", create time ")
                .appendDescriptionOf(createTimeMatcher)
                .appendText(", start time ")
                .appendDescriptionOf(startTimeMatcher)
                .appendText(" and finish time ")
                .appendDescriptionOf(finishTimeMatcher);
    }
}
