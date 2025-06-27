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
 * Matcher for {@link JobState}.
 */
public class RestJobStateMatcher extends TypeSafeMatcher<JobState> {
    private Matcher<JobStatus> statusMatcher = AnythingMatcher.anything();
    private Matcher<UUID> idMatcher = AnythingMatcher.anything();
    private Matcher<Instant> createTimeMatcher = AnythingMatcher.anything();
    private Matcher<Instant> startTimeMatcher = AnythingMatcher.anything();
    private Matcher<Instant> finishTimeMatcher = AnythingMatcher.anything();

    public static RestJobStateMatcher isJobState() {
        return new RestJobStateMatcher();
    }

    public static RestJobStateMatcher queued(UUID id) {
        return queued(equalTo(id));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobState} has a status of {@link JobStatus#QUEUED}.
     *
     * @param idMatcher Id matcher.
     * @return Matcher.
     */
    public static RestJobStateMatcher queued(Matcher<UUID> idMatcher) {
        return isJobState().withId(idMatcher)
                .withStatus(JobStatus.QUEUED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(nullValue(Instant.class));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobState} has a status of {@link JobStatus#EXECUTING}.
     *
     * @param id Id.
     * @return Matcher.
     */
    public static RestJobStateMatcher executing(UUID id) {
        return isJobState().withId(id)
                .withStatus(JobStatus.EXECUTING)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(notNullValue(Instant.class))
                .withFinishTime(nullValue(Instant.class));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobState} has a status of {@link JobStatus#FAILED}.
     *
     * @param id Id.
     * @param wasRunning Whether the job was running before it failed.
     * @return Matcher.
     */
    public static RestJobStateMatcher failed(UUID id, boolean wasRunning) {
        return isJobState().withId(id)
                .withStatus(JobStatus.FAILED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(wasRunning ? notNullValue(Instant.class) : AnythingMatcher.anything())
                .withFinishTime(notNullValue(Instant.class));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobState} has a status of {@link JobStatus#COMPLETED}.
     *
     * @param id Id.
     * @return Matcher.
     */
    public static RestJobStateMatcher completed(UUID id) {
        return isJobState().withId(id)
                .withStatus(JobStatus.COMPLETED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(notNullValue(Instant.class))
                .withFinishTime(notNullValue(Instant.class));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobState} has a status of {@link JobStatus#CANCELING}.
     *
     * @param id Id.
     * @param wasRunning Whether the job was running before it was canceled.
     * @return Matcher.
     */
    public static RestJobStateMatcher canceling(UUID id, boolean wasRunning) {
        return isJobState().withId(id)
                .withStatus(JobStatus.CANCELING)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(wasRunning ? notNullValue(Instant.class) : AnythingMatcher.anything())
                .withFinishTime(notNullValue(Instant.class));
    }

    /**
     * Creates a matcher that matches when the examined {@link JobState} has a status of {@link JobStatus#CANCELED}.
     *
     * @param id Id.
     * @param wasRunning Whether the job was running before it was canceled.
     * @return Matcher.
     */
    public static RestJobStateMatcher canceled(UUID id, boolean wasRunning) {
        return isJobState().withId(id)
                .withStatus(JobStatus.CANCELED)
                .withCreateTime(notNullValue(Instant.class))
                .withStartTime(wasRunning ? notNullValue(Instant.class) : AnythingMatcher.anything())
                .withFinishTime(notNullValue(Instant.class));
    }


    public RestJobStateMatcher withStatus(JobStatus status) {
        return withStatus(equalTo(status));
    }

    public RestJobStateMatcher withStatus(Matcher<JobStatus> statusMatcher) {
        this.statusMatcher = statusMatcher;
        return this;
    }


    public RestJobStateMatcher withId(UUID id) {
        return withId(equalTo(id));
    }

    public RestJobStateMatcher withId(Matcher<UUID> idMatcher) {
        this.idMatcher = idMatcher;
        return this;
    }

    public RestJobStateMatcher withCreateTime(Instant createTime) {
        return withCreateTime(equalTo(createTime));
    }

    public RestJobStateMatcher withCreateTime(Matcher<Instant> createTimeMatcher) {
        this.createTimeMatcher = createTimeMatcher;
        return this;
    }

    public RestJobStateMatcher withStartTime(Instant startTime) {
        return withStartTime(equalTo(startTime));
    }

    public RestJobStateMatcher withStartTime(Matcher<Instant> startTimeMatcher) {
        this.startTimeMatcher = startTimeMatcher;
        return this;
    }

    public RestJobStateMatcher withFinishTime(Instant finishTime) {
        return withFinishTime(equalTo(finishTime));
    }

    public RestJobStateMatcher withFinishTime(Matcher<Instant> finishTimeMatcher) {
        this.finishTimeMatcher = finishTimeMatcher;
        return this;
    }

    @Override
    protected boolean matchesSafely(JobState state) {
        return idMatcher.matches(state.id())
                && statusMatcher.matches(state.status())
                && createTimeMatcher.matches(state.createTime())
                && startTimeMatcher.matches(state.startTime())
                && finishTimeMatcher.matches(state.finishTime());
    }

    @Override
    protected void describeMismatchSafely(JobState state, Description mismatchDescription) {
        mismatchDescription.appendText("was a JobState with id ")
                .appendValue(state.id())
                .appendText(", status ")
                .appendValue(state.status())
                .appendText(", create time ")
                .appendValue(state.createTime())
                .appendText(", start time ")
                .appendValue(state.startTime())
                .appendText(" and finish time ")
                .appendValue(state.finishTime());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a JobState with id ")
                .appendDescriptionOf(idMatcher)
                .appendText(", status ")
                .appendDescriptionOf(statusMatcher)
                .appendText(", create time ")
                .appendDescriptionOf(createTimeMatcher)
                .appendText(", start time ")
                .appendDescriptionOf(startTimeMatcher)
                .appendText(" and finish time ")
                .appendDescriptionOf(finishTimeMatcher);
    }
}
