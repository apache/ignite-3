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

package org.apache.ignite.internal.compute;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/**
 * Fail-safe wrapper for the {@link JobExecution} that should be returned to the client. This wrapper holds the original job execution
 * object. This object can be updated during the lifetime of {@link FailSafeJobExecution}.
 *
 * <p>The problem that is solved by this wrapper is the following: client can join the {@link JobExecution#resultAsync()}
 * future but this original future will never be completed in case the remote worker node has left the topology. By returning
 * {@link FailSafeJobExecution} to the client we can update the original job execution object when it is restarted on another node but the
 * client will still be able to join the original future.
 *
 * @param <T> the type of the job result.
 */
class FailSafeJobExecution<T> implements JobExecution<T> {
    private static final IgniteLogger LOG = Loggers.forClass(FailSafeJobExecution.class);

    /**
     * Exception that was thrown during the job execution. It can be set only once.
     */
    private final AtomicReference<Throwable> exception = new AtomicReference<>(null);

    /**
     * The future that is returned as {@link JobExecution#resultAsync()} and will be resolved when the job is completed.
     */
    private final CompletableFuture<T> resultFuture;

    /**
     * The status of the first job execution attempt. It is used to preserve the original job creation time.
     */
    private final AtomicReference<JobStatus> capturedStatus;

    /**
     * Link to the current job execution object. It can be updated when the job is restarted on another node.
     */
    private final AtomicReference<JobExecution<T>> runningJobExecution;

    FailSafeJobExecution(JobExecution<T> runningJobExecution) throws RuntimeException {
        this.resultFuture = new CompletableFuture<>();
        this.runningJobExecution = new AtomicReference<>(runningJobExecution);

        this.capturedStatus = new AtomicReference<>(null);
        captureStatus(runningJobExecution);

        registerCompleteHook();
    }

    private void captureStatus(JobExecution<T> runningJobExecution) {
        runningJobExecution.statusAsync()
                .completeOnTimeout(failedStatus(), 10, TimeUnit.SECONDS)
                .whenComplete((status, e) -> {
                    if (status != null) {
                        this.capturedStatus.compareAndSet(null, status);
                    } else {
                        this.capturedStatus.compareAndSet(null, failedStatus());
                    }
                });
    }

    private static JobStatus failedStatus() {
        return JobStatusImpl.builder().id(UUID.randomUUID()).createTime(Instant.now()).state(JobState.FAILED).build();
    }

    /**
     * Registers a hook for the future that is returned to the user. This future will be completed when the job is completed.
     */
    private void registerCompleteHook() {
        runningJobExecution.get().resultAsync().whenComplete((res, err) -> {
            if (err == null) {
                resultFuture.complete(res);
            } else {
                resultFuture.completeExceptionally(err);
            }
        });
    }

    void updateJobExecution(JobExecution<T> jobExecution) {
        LOG.debug("Updating job execution: {}", jobExecution);

        runningJobExecution.set(jobExecution);
        registerCompleteHook();
    }

    /**
     * Transforms the status by modifying the fields that should be always the same regardless of the job execution attempt. For example,
     * the job creation time should be the same for all attempts.
     *
     * <p>Can update {@link #capturedStatus} as a side-effect if the one is null.
     *
     * @param jobStatus current job status.
     * @return transformed job status.
     */
    private @Nullable JobStatus transformStatus(@Nullable JobStatus jobStatus) {
        if (jobStatus == null) {
            return null;
        }

        if (capturedStatus.get() == null) {
            capturedStatus.compareAndSet(null, jobStatus);
        }

        return JobStatusImpl.toBuilder(jobStatus)
                .createTime(capturedStatus.get().createTime())
                .id(capturedStatus.get().id())
                .build();
    }

    @Override
    public CompletableFuture<T> resultAsync() {
        return resultFuture;
    }

    /**
     * Returns the transformed status of the running job execution. The transformation is needed because we do not want to change some
     * fields of the status (e.g. creation time) when the job is restarted.
     *
     * @return the transformed status.
     */
    @Override
    public CompletableFuture<@Nullable JobStatus> statusAsync() {
        if (exception.get() != null) {
            return CompletableFuture.failedFuture(exception.get());
        }

        return runningJobExecution.get()
                .statusAsync()
                .thenApply(this::transformStatus);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        resultFuture.cancel(false);
        return runningJobExecution.get().cancelAsync();
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        return runningJobExecution.get().changePriorityAsync(newPriority);
    }

    /**
     * Completes the future with the exception. This method can be called only once.
     *
     * @param ex the exception that should be set to the future.
     */
    void completeExceptionally(Exception ex) {
        if (exception.compareAndSet(null, ex)) {
            runningJobExecution.get().resultAsync().completeExceptionally(ex);
            resultFuture.completeExceptionally(ex);
        } else {
            throw new IllegalStateException("Job is already completed exceptionally.");
        }
    }
}
