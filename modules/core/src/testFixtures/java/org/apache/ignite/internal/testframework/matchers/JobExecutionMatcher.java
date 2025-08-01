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
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.network.ClusterNode;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.Nullable;

/**
 * Matcher for {@link JobExecution}.
 */
public class JobExecutionMatcher<R> extends TypeSafeMatcher<JobExecution<R>> {
    private final Matcher<? super CompletableFuture<R>> resultMatcher;
    private final Matcher<? super CompletableFuture<JobState>> stateMatcher;
    private final Matcher<? super String> nodeNameMatcher;

    private JobExecutionMatcher(
            @Nullable Matcher<? super CompletableFuture<R>> resultMatcher,
            @Nullable Matcher<? super CompletableFuture<JobState>> stateMatcher,
            @Nullable Matcher<? super String> nodeNameMatcher
    ) {
        this.resultMatcher = resultMatcher;
        this.stateMatcher = stateMatcher;
        this.nodeNameMatcher = nodeNameMatcher;
    }

    public static <R> JobExecutionMatcher<R> jobExecutionWithStatus(JobStatus status) {
        return new JobExecutionMatcher<>(null, will(jobStateWithStatus(status)), null);
    }

    public static <R> JobExecutionMatcher<R> jobExecutionWithResultAndStatus(R result, JobStatus status) {
        return new JobExecutionMatcher<>(willBe(result), will(jobStateWithStatus(status)), null);
    }

    public static <R> JobExecutionMatcher<R> jobExecutionWithResultAndStatus(
            Matcher<R> resultMatcher,
            Matcher<JobStatus> statusMatcher
    ) {
        return new JobExecutionMatcher<>(will(resultMatcher), will(jobStateWithStatus(statusMatcher)), null);
    }

    public static <R> JobExecutionMatcher<R> jobExecutionWithResultAndNode(R result, ClusterNode node) {
        return new JobExecutionMatcher<>(willBe(result), null, is(node.name()));
    }

    public static <R> JobExecutionMatcher<R> jobExecutionWithResultStatusAndNode(R result, JobStatus status, ClusterNode node) {
        return new JobExecutionMatcher<>(willBe(result), will(jobStateWithStatus(status)), is(node.name()));
    }

    public static <R> JobExecutionMatcher<R> jobExecutionWithResultAndStateFuture(
            Matcher<? super CompletableFuture<R>> resultMatcher,
            Matcher<? super CompletableFuture<JobState>> stateMatcher
    ) {
        return new JobExecutionMatcher<>(resultMatcher, stateMatcher, null);
    }

    @Override
    protected boolean matchesSafely(JobExecution<R> execution) {
        if (resultMatcher != null && !resultMatcher.matches(execution.resultAsync())) {
            return false;
        }
        if (stateMatcher != null && !stateMatcher.matches(execution.stateAsync())) {
            return false;
        }
        return nodeNameMatcher == null || nodeNameMatcher.matches(execution.node().name());
    }

    @Override
    protected void describeMismatchSafely(JobExecution<R> execution, Description mismatchDescription) {
        if (resultMatcher != null) {
            mismatchDescription.appendText("result ");
            resultMatcher.describeMismatch(execution.resultAsync(), mismatchDescription);
        }
        if (stateMatcher != null) {
            if (resultMatcher != null) {
                mismatchDescription.appendText(", ");
            }
            mismatchDescription.appendText("state ");
            stateMatcher.describeMismatch(execution.stateAsync(), mismatchDescription);
        }
        if (nodeNameMatcher != null) {
            if (resultMatcher != null || stateMatcher != null) {
                mismatchDescription.appendText(", ");
            }
            mismatchDescription.appendText("node ");
            nodeNameMatcher.describeMismatch(execution.node().name(), mismatchDescription);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a JobExecution with ");
        if (resultMatcher != null) {
            description.appendText("result ").appendDescriptionOf(resultMatcher);
        }
        if (stateMatcher != null) {
            if (resultMatcher != null) {
                description.appendText(" and ");
            }
            description.appendText("state ").appendDescriptionOf(stateMatcher);
        }
        if (nodeNameMatcher != null) {
            if (resultMatcher != null || stateMatcher != null) {
                description.appendText(" and ");
            }
            description.appendText("node ").appendDescriptionOf(nodeNameMatcher);
        }
    }
}
