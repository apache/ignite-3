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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for {@link JobExecution}.
 */
public class JobExecutionMatcher<R> extends TypeSafeMatcher<JobExecution<R>> {
    private final Matcher<? super CompletableFuture<R>> resultMatcher;
    private final Matcher<? super CompletableFuture<JobState>> stateMatcher;

    private JobExecutionMatcher(
            Matcher<? super CompletableFuture<R>> resultMatcher,
            Matcher<? super CompletableFuture<JobState>> stateMatcher
    ) {
        this.resultMatcher = resultMatcher;
        this.stateMatcher = stateMatcher;
    }

    public static <R> JobExecutionMatcher<R> jobExecutionWithResultAndStatus(R result, JobStatus status) {
        return new JobExecutionMatcher<>(willBe(result), will(JobStateMatcher.jobStateWithStatus(status)));
    }

    public static <R> JobExecutionMatcher<R> jobExecutionWithResultAndStatus(
            Matcher<R> resultMatcher,
            Matcher<JobStatus> statusMatcher
    ) {
        return new JobExecutionMatcher<>(will(resultMatcher), will(JobStateMatcher.jobStateWithStatus(statusMatcher)));
    }

    public static <R> JobExecutionMatcher<R> jobExecutionWithResultAndStateFuture(
            Matcher<? super CompletableFuture<R>> resultMatcher,
            Matcher<? super CompletableFuture<JobState>> stateMatcher
    ) {
        return new JobExecutionMatcher<>(resultMatcher, stateMatcher);
    }

    @Override
    protected boolean matchesSafely(JobExecution<R> execution) {
        return resultMatcher.matches(execution.resultAsync()) && stateMatcher.matches(execution.stateAsync());
    }

    @Override
    protected void describeMismatchSafely(JobExecution<R> execution, Description mismatchDescription) {
        mismatchDescription.appendText("result ");
        resultMatcher.describeMismatch(execution.resultAsync(), mismatchDescription);
        mismatchDescription.appendText(", state ");
        stateMatcher.describeMismatch(execution.stateAsync(), mismatchDescription);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a JobExecution with result ")
                .appendDescriptionOf(resultMatcher)
                .appendText(" and state ")
                .appendDescriptionOf(stateMatcher);
    }
}
