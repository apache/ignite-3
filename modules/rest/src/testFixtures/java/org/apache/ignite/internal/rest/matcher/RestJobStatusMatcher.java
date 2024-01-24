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
    private Matcher<JobState> stateMatcher = AnythingMatcher.anything();
    private Matcher<UUID> idMatcher = AnythingMatcher.anything();
    private Matcher<Instant> createTimeMatcher = AnythingMatcher.anything();
    private Matcher<Instant> startTimeMatcher = AnythingMatcher.anything();
    private Matcher<Instant> finishTimeMatcher = AnythingMatcher.anything();

    public static RestJobStatusMatcher isJobStatus() {
        return new RestJobStatusMatcher();
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
        return isJobStatus().withId(idMatcher)
                .withState(JobState.QUEUED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(nullValue(Instant.class));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobStatus} has a state of {@link JobState#EXECUTING}.
     *
     * @param id Id.
     * @return Matcher.
     */
    public static RestJobStatusMatcher executing(UUID id) {
        return isJobStatus().withId(id)
                .withState(JobState.EXECUTING)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(notNullValue(Instant.class))
                .withFinishTime(nullValue(Instant.class));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobStatus} has a state of {@link JobState#FAILED}.
     *
     * @param id Id.
     * @param wasRunning Whether the job was running before it failed.
     * @return Matcher.
     */
    public static RestJobStatusMatcher failed(UUID id, boolean wasRunning) {
        return isJobStatus().withId(id)
                .withState(JobState.FAILED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(wasRunning ? notNullValue(Instant.class) : AnythingMatcher.anything())
                .withFinishTime(notNullValue(Instant.class));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobStatus} has a state of {@link JobState#COMPLETED}.
     *
     * @param id Id.
     * @return Matcher.
     */
    public static RestJobStatusMatcher completed(UUID id) {
        return isJobStatus().withId(id)
                .withState(JobState.COMPLETED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(notNullValue(Instant.class))
                .withFinishTime(notNullValue(Instant.class));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobStatus} has a state of {@link JobState#CANCELING}.
     *
     * @param id Id.
     * @param wasRunning Whether the job was running before it was canceled.
     * @return Matcher.
     */
    public static RestJobStatusMatcher canceling(UUID id, boolean wasRunning) {
        return isJobStatus().withId(id)
                .withState(JobState.CANCELING)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(wasRunning ? notNullValue(Instant.class) : AnythingMatcher.anything())
                .withFinishTime(notNullValue(Instant.class));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobStatus} has a state of {@link JobState#CANCELED}.
     *
     * @param id Id.
     * @param wasRunning Whether the job was running before it was canceled.
     * @return Matcher.
     */
    public static RestJobStatusMatcher canceled(UUID id, boolean wasRunning) {
        return isJobStatus().withId(id)
                .withState(JobState.CANCELED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(wasRunning ? notNullValue(Instant.class) : AnythingMatcher.anything())
                .withFinishTime(notNullValue(Instant.class));
    }


    public RestJobStatusMatcher withState(JobState state) {
        return withState(equalTo(state));
    }

    public RestJobStatusMatcher withState(Matcher<JobState> stateMatcher) {
        this.stateMatcher = stateMatcher;
        return this;
    }


    public RestJobStatusMatcher withId(UUID id) {
        return withId(equalTo(id));
    }

    public RestJobStatusMatcher withId(Matcher<UUID> idMatcher) {
        this.idMatcher = idMatcher;
        return this;
    }

    public RestJobStatusMatcher withCreateTime(Instant createTime) {
        return withCreateTime(equalTo(createTime));
    }

    public RestJobStatusMatcher withCreateTime(Matcher<Instant> createTimeMatcher) {
        this.createTimeMatcher = createTimeMatcher;
        return this;
    }

    public RestJobStatusMatcher withStartTime(Instant startTime) {
        return withStartTime(equalTo(startTime));
    }

    public RestJobStatusMatcher withStartTime(Matcher<Instant> startTimeMatcher) {
        this.startTimeMatcher = startTimeMatcher;
        return this;
    }

    public RestJobStatusMatcher withFinishTime(Instant finishTime) {
        return withFinishTime(equalTo(finishTime));
    }

    public RestJobStatusMatcher withFinishTime(Matcher<Instant> finishTimeMatcher) {
        this.finishTimeMatcher = finishTimeMatcher;
        return this;
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
        mismatchDescription.appendText("was a JobStatus with id ")
                .appendValue(status.id())
                .appendText(", state ")
                .appendValue(status.state())
                .appendText(", create time ")
                .appendValue(status.createTime())
                .appendText(", start time ")
                .appendValue(status.startTime())
                .appendText(" and finish time ")
                .appendValue(status.finishTime());
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
}
