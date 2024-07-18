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

package org.apache.ignite.internal.compute.utils;

import static org.apache.ignite.compute.JobStatus.CANCELED;
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.compute.JobStatus.QUEUED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.lang.IgniteException;

/**
 * Testing instance of {@link JobExecution}. Adds useful assertions on job's state and sync methods.
 *
 * @param <R> Job result type.
 */
public class TestingJobExecution<R> implements JobExecution<R> {
    private final JobExecution<R> jobExecution;

    /**
     * Constructor.
     *
     * @param jobExecution job execution to wrap.
     */
    public TestingJobExecution(JobExecution<R> jobExecution) {
        this.jobExecution = jobExecution;
    }

    private JobState stateSync() throws InterruptedException, ExecutionException, TimeoutException {
        return jobExecution.stateAsync().get(10, TimeUnit.SECONDS);
    }

    public UUID idSync() throws InterruptedException, ExecutionException, TimeoutException {
        return jobExecution.idAsync().get(10, TimeUnit.SECONDS);
    }

    private R resultSync() throws ExecutionException, InterruptedException, TimeoutException {
        return jobExecution.resultAsync().get(10, TimeUnit.SECONDS);
    }

    public long createTimeMillis() throws ExecutionException, InterruptedException, TimeoutException {
        return stateSync().createTime().toEpochMilli();
    }

    public long startTimeMillis() throws ExecutionException, InterruptedException, TimeoutException {
        return stateSync().startTime().toEpochMilli();
    }

    public long finishTimeMillis() throws ExecutionException, InterruptedException, TimeoutException {
        return stateSync().finishTime().toEpochMilli();
    }

    public void cancelSync() throws ExecutionException, InterruptedException, TimeoutException {
        jobExecution.cancelAsync().get(10, TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return jobExecution.resultAsync();
    }

    @Override
    public CompletableFuture<JobState> stateAsync() {
        return jobExecution.stateAsync();
    }

    @Override
    public CompletableFuture<Boolean> cancelAsync() {
        return jobExecution.cancelAsync();
    }

    @Override
    public CompletableFuture<Boolean> changePriorityAsync(int newPriority) {
        return jobExecution.changePriorityAsync(newPriority);
    }

    /**
     * Checks that the job execution object has EXECUTING state.
     */
    public void assertQueued() {
        await().until(jobExecution::stateAsync, willBe(jobStateWithStatus(QUEUED)));

        assertThat(resultAsync().isDone(), equalTo(false));

        assertThat(idAsync(), willBe(notNullValue()));
    }

    /**
     * Checks that the job execution object has EXECUTING state.
     */
    public void assertExecuting() {
        await().until(jobExecution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(resultAsync().isDone(), equalTo(false));

        assertThat(idAsync(), willBe(notNullValue()));
    }

    /**
     * Checks that the job execution object is cancelled.
     */
    public void assertCancelled(Class<? extends Exception> exceptionClass) {
        await().until(jobExecution::stateAsync, willBe(jobStateWithStatus(CANCELED)));

        assertThat(resultAsync(), willThrow(exceptionClass));
    }

    /**
     * Checks that the job execution object is completed successfully.
     */
    public void assertCompleted() {
        await().until(jobExecution::stateAsync, willBe(jobStateWithStatus(COMPLETED)));

        assertThat(resultAsync(), willBe("Done"));
    }

    /**
     * Checks that the job execution has failed.
     */
    public void assertFailed() {
        await().untilAsserted(() -> {
            assertThat(jobExecution.resultAsync(), willThrow(IgniteException.class));
        });
        await().untilAsserted(() -> {
            assertThat(jobExecution.stateAsync(), willThrow(IgniteException.class));
        });
    }
}
