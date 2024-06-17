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

package org.apache.ignite.internal.compute.task;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.compute.JobState.CANCELED;
import static org.apache.ignite.compute.JobState.COMPLETED;
import static org.apache.ignite.compute.JobState.EXECUTING;
import static org.apache.ignite.compute.JobState.FAILED;
import static org.apache.ignite.internal.compute.ComputeUtils.instantiateTask;
import static org.apache.ignite.internal.util.ArrayUtils.concat;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.task.ComputeJobRunner;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.internal.compute.queue.PriorityQueueExecutor;
import org.apache.ignite.internal.compute.queue.QueueExecution;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/**
 * Internal map reduce task execution object. Runs the {@link MapReduceTask#split(TaskExecutionContext, Object...)} method of the task as a
 * compute job, then submits the resulting list of jobs. Waits for completion of all compute jobs, then submits the
 * {@link MapReduceTask#reduce(Map)} method as a compute job. The result of the task is the result of the split method.
 *
 * @param <R> Task result type.
 */
@SuppressWarnings("unchecked")
public class TaskExecutionInternal<R> implements JobExecution<R> {
    private static final IgniteLogger LOG = Loggers.forClass(TaskExecutionInternal.class);

    private final QueueExecution<SplitResult<R>> splitExecution;

    private final CompletableFuture<List<JobExecution<Object>>> executionsFuture;

    private final CompletableFuture<Map<UUID, Object>> resultsFuture;

    private final CompletableFuture<QueueExecution<R>> reduceExecutionFuture;

    private final AtomicReference<JobStatus> reduceFailedStatus = new AtomicReference<>();

    /**
     * Construct an execution object and starts executing.
     *
     * @param executorService Compute jobs executor.
     * @param jobSubmitter Compute jobs submitter.
     * @param taskClass Map reduce task class.
     * @param context Task execution context.
     * @param args Task arguments.
     */
    public TaskExecutionInternal(
            PriorityQueueExecutor executorService,
            JobSubmitter jobSubmitter,
            Class<? extends MapReduceTask<R>> taskClass,
            TaskExecutionContext context,
            Object... args
    ) {
        LOG.debug("Executing task {}", taskClass.getName());
        splitExecution = executorService.submit(
                () -> {
                    MapReduceTask<R> task = instantiateTask(taskClass);

                    return completedFuture(new SplitResult<>(task, task.split(context, args)));
                },
                Integer.MAX_VALUE,
                0
        );

        executionsFuture = splitExecution.resultAsync().thenApply(splitResult -> {
            List<ComputeJobRunner> runners = splitResult.runners();
            LOG.debug("Submitting {} jobs for {}", runners.size(), taskClass.getName());
            return submit(runners, jobSubmitter);
        });

        resultsFuture = executionsFuture.thenCompose(TaskExecutionInternal::resultsAsync);

        reduceExecutionFuture = resultsFuture.thenApply(results -> {
            LOG.debug("Running reduce job for {}", taskClass.getName());

            // This future is already finished
            MapReduceTask<R> task = splitExecution.resultAsync().thenApply(SplitResult::task).join();

            return executorService.submit(
                    () -> completedFuture(task.reduce(results)),
                    Integer.MAX_VALUE,
                    0
            );
        }).whenComplete(this::captureReduceFailure);
    }

    private void captureReduceFailure(QueueExecution<R> reduceExecution, Throwable throwable) {
        if (throwable != null) {
            // Capture the reduce execution failure reason and time.
            JobState state = throwable instanceof CancellationException ? CANCELED : FAILED;
            reduceFailedStatus.set(
                    splitExecution.status().toBuilder()
                            .state(state)
                            .finishTime(Instant.now())
                            .build()
            );
        }
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return reduceExecutionFuture.thenCompose(QueueExecution::resultAsync);
    }

    @Override
    public CompletableFuture<@Nullable JobStatus> statusAsync() {
        JobStatus splitStatus = splitExecution.status();
        if (splitStatus == null) {
            // Return null even if the reduce execution can still be retained.
            return nullCompletedFuture();
        }

        if (splitStatus.state() != COMPLETED) {
            return completedFuture(splitStatus);
        }

        // This future is complete when reduce execution job is submitted, return status from it.
        if (reduceExecutionFuture.isDone()) {
            return reduceExecutionFuture.handle((reduceExecution, throwable) -> {
                if (throwable == null) {
                    JobStatus reduceStatus = reduceExecution.status();
                    if (reduceStatus == null) {
                        return null;
                    }
                    return reduceStatus.toBuilder()
                            .id(splitStatus.id())
                            .createTime(splitStatus.createTime())
                            .startTime(splitStatus.startTime())
                            .build();
                }
                return reduceFailedStatus.get();
            });
        }

        // At this point split is complete but reduce job is not submitted yet.
        return completedFuture(splitStatus.toBuilder()
                .state(EXECUTING)
                .finishTime(null)
                .build());
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        // If the split job is not complete, this will cancel the executions future.
        splitExecution.cancel();

        // This means we didn't submit any jobs yet.
        if (executionsFuture.cancel(true)) {
            return trueCompletedFuture();
        }

        // Split job was complete, results future was running, but not complete yet.
        if (resultsFuture.cancel(true)) {
            return executionsFuture.thenCompose(executions -> {
                CompletableFuture<Boolean>[] cancelFutures = executions.stream()
                        .map(JobExecution::cancelAsync)
                        .toArray(CompletableFuture[]::new);

                return allOf(cancelFutures);
            }).thenApply(unused -> true);
        }

        // Results arrived but reduce is not yet submitted
        if (reduceExecutionFuture.cancel(true)) {
            return trueCompletedFuture();
        }

        return reduceExecutionFuture.thenApply(QueueExecution::cancel);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        // If the split job is not started
        if (splitExecution.changePriority(newPriority)) {
            return trueCompletedFuture();
        }

        // This future is complete when reduce execution job is submitted, try to change its priority.
        if (reduceExecutionFuture.isDone()) {
            return reduceExecutionFuture.thenApply(reduceExecution -> reduceExecution.changePriority(newPriority));
        }

        return executionsFuture.thenCompose(executions -> {
            CompletableFuture<Boolean>[] changePriorityFutures = executions.stream()
                    .map(execution -> execution.changePriorityAsync(newPriority))
                    .toArray(CompletableFuture[]::new);

            return allOf(changePriorityFutures).thenApply(unused -> {
                List<@Nullable Boolean> results = Arrays.stream(changePriorityFutures).map(CompletableFuture::join).collect(toList());
                if (results.stream().allMatch(b -> b == Boolean.TRUE)) {
                    return true;
                }
                if (results.stream().anyMatch(Objects::isNull)) {
                    //noinspection RedundantCast this cast is to satisfy spotbugs
                    return (Boolean) null;
                }
                return false;
            });
        });
    }

    CompletableFuture<List<@Nullable JobStatus>> statusesAsync() {
        return executionsFuture.thenCompose(executions -> {
            CompletableFuture<JobStatus>[] statusFutures = executions.stream()
                    .map(JobExecution::statusAsync)
                    .toArray(CompletableFuture[]::new);

            return allOfToList(statusFutures);
        });

    }

    private static CompletableFuture<Map<UUID, Object>> resultsAsync(List<JobExecution<Object>> executions) {
        CompletableFuture<?>[] resultFutures = executions.stream()
                .map(JobExecution::resultAsync)
                .toArray(CompletableFuture[]::new);

        CompletableFuture<UUID>[] idFutures = executions.stream()
                .map(JobExecution::idAsync)
                .toArray(CompletableFuture[]::new);

        return allOf(concat(resultFutures, idFutures)).thenApply(unused -> {
            Map<UUID, Object> results = new HashMap<>();

            for (int i = 0; i < resultFutures.length; i++) {
                results.put(idFutures[i].join(), resultFutures[i].join());
            }

            return results;
        });
    }

    private static <R> List<JobExecution<Object>> submit(List<ComputeJobRunner> runners, JobSubmitter jobSubmitter) {
        return runners.stream()
                .map(jobSubmitter::submit)
                .collect(toList());
    }

    private static class SplitResult<R> {
        private final MapReduceTask<R> task;

        private final List<ComputeJobRunner> runners;

        private SplitResult(MapReduceTask<R> task, List<ComputeJobRunner> runners) {
            this.task = task;
            this.runners = runners;
        }

        private List<ComputeJobRunner> runners() {
            return runners;
        }

        private MapReduceTask<R> task() {
            return task;
        }
    }
}
