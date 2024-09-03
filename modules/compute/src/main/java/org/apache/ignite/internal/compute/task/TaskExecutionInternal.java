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
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.TaskStatus.CANCELED;
import static org.apache.ignite.compute.TaskStatus.EXECUTING;
import static org.apache.ignite.compute.TaskStatus.FAILED;
import static org.apache.ignite.internal.compute.ComputeUtils.instantiateTask;
import static org.apache.ignite.internal.util.ArrayUtils.concat;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.marshalling.Marshaller.tryUnmarshalOrCast;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.TaskState;
import org.apache.ignite.compute.TaskStatus;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.internal.compute.MarshallerProvider;
import org.apache.ignite.internal.compute.TaskStateImpl;
import org.apache.ignite.internal.compute.queue.PriorityQueueExecutor;
import org.apache.ignite.internal.compute.queue.QueueExecution;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.marshalling.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Internal map reduce task execution object. Runs the {@link MapReduceTask#splitAsync(TaskExecutionContext, Object)} method of the task
 * as a compute job, then submits the resulting list of jobs. Waits for completion of all compute jobs, then submits the
 * {@link MapReduceTask#reduceAsync(TaskExecutionContext, Map)} method as a compute job. The result of the task is the result of the split
 * method.
 *
 * @param <R> Task result type.
 */
@SuppressWarnings("unchecked")
public class TaskExecutionInternal<I, M, T, R> implements TaskExecution<R>, MarshallerProvider<R> {
    private static final IgniteLogger LOG = Loggers.forClass(TaskExecutionInternal.class);

    private final QueueExecution<SplitResult<I, M, T, R>> splitExecution;

    private final CompletableFuture<List<JobExecution<T>>> executionsFuture;

    private final CompletableFuture<Map<UUID, T>> resultsFuture;

    private final CompletableFuture<QueueExecution<R>> reduceExecutionFuture;

    private final AtomicReference<TaskState> reduceFailedState = new AtomicReference<>();

    private final AtomicBoolean isCancelled;

    private volatile @Nullable Marshaller<R, byte[]> reduceResultMarshallerRef;

    /**
     * Construct an execution object and starts executing.
     *
     * @param executorService Compute jobs executor.
     * @param jobSubmitter Compute jobs submitter.
     * @param taskClass Map reduce task class.
     * @param context Task execution context.
     * @param isCancelled Flag which is passed to the execution context so that the task can check it for cancellation request.
     * @param arg Task argument.
     */
    public TaskExecutionInternal(
            PriorityQueueExecutor executorService,
            JobSubmitter jobSubmitter,
            Class<? extends MapReduceTask<I, M, T, R>> taskClass,
            TaskExecutionContext context,
            AtomicBoolean isCancelled,
            I arg
    ) {
        this.isCancelled = isCancelled;
        LOG.debug("Executing task {}", taskClass.getName());
        splitExecution = executorService.submit(
                () -> {
                    MapReduceTask<I, M, T, R> task = instantiateTask(taskClass);

                    reduceResultMarshallerRef = task.reduceJobResultMarshaller();

                    return task.splitAsync(context, tryUnmarshalOrCast(task.splitJobInputMarshaller(), arg))
                            .thenApply(jobs -> new SplitResult<>(task, jobs));
                },

                Integer.MAX_VALUE,
                0
        );

        executionsFuture = splitExecution.resultAsync().thenApply(splitResult -> {
            List<MapReduceJob<M, T>> runners = splitResult.runners();
            LOG.debug("Submitting {} jobs for {}", runners.size(), taskClass.getName());
            return submit(runners, jobSubmitter);
        });

        resultsFuture = executionsFuture.thenCompose(TaskExecutionInternal::resultsAsync);

        reduceExecutionFuture = resultsFuture.thenApply(results -> {
            LOG.debug("Running reduce job for {}", taskClass.getName());

            // This future is already finished
            MapReduceTask<I, M, T, R> task = splitExecution.resultAsync().thenApply(SplitResult::task).join();

            return executorService.submit(
                    () -> task.reduceAsync(context, results),
                    Integer.MAX_VALUE,
                    0
            );
        }).whenComplete(this::captureReduceFailure);
    }

    private void captureReduceFailure(QueueExecution<R> reduceExecution, Throwable throwable) {
        if (throwable != null) {
            // Capture the reduce execution failure reason and time.
            TaskStatus status = throwable instanceof CancellationException ? CANCELED : FAILED;

            JobState state = splitExecution.state();
            if (state != null) {
                reduceFailedState.set(
                        TaskStateImpl.toBuilder(state)
                                .status(status)
                                .finishTime(Instant.now())
                                .build()
                );
            }
        }
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return reduceExecutionFuture.thenCompose(QueueExecution::resultAsync);
    }

    @Override
    public CompletableFuture<@Nullable TaskState> stateAsync() {
        JobState splitState = splitExecution.state();
        if (splitState == null) {
            // Return null even if the reduce execution can still be retained.
            return nullCompletedFuture();
        }

        if (splitState.status() != COMPLETED) {
            return completedFuture(TaskStateImpl.toBuilder(splitState).build());
        }

        // This future is complete when reduce execution job is submitted, return status from it.
        if (reduceExecutionFuture.isDone()) {
            return reduceExecutionFuture.handle((reduceExecution, throwable) -> {
                if (throwable == null) {
                    JobState reduceState = reduceExecution.state();
                    if (reduceState == null) {
                        return null;
                    }
                    return TaskStateImpl.toBuilder(reduceState)
                            .id(splitState.id())
                            .createTime(splitState.createTime())
                            .startTime(splitState.startTime())
                            .build();
                }
                return reduceFailedState.get();
            });
        }

        // At this point split is complete but reduce job is not submitted yet.
        return completedFuture(TaskStateImpl.toBuilder(splitState)
                .status(EXECUTING)
                .finishTime(null)
                .build());
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        if (!isCancelled.compareAndSet(false, true)) {
            return falseCompletedFuture();
        }

        // If the split job is not complete, this will cancel the executions future.
        if (splitExecution.cancel()) {
            return trueCompletedFuture();
        }

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

    @Override
    public CompletableFuture<List<@Nullable JobState>> statesAsync() {
        return executionsFuture.thenCompose(executions -> {
            CompletableFuture<JobState>[] stateFutures = executions.stream()
                    .map(JobExecution::stateAsync)
                    .toArray(CompletableFuture[]::new);

            return allOfToList(stateFutures);
        });

    }

    private static <T> CompletableFuture<Map<UUID, T>> resultsAsync(List<JobExecution<T>> executions) {
        CompletableFuture<T>[] resultFutures = executions.stream()
                .map(JobExecution::resultAsync)
                .toArray(CompletableFuture[]::new);

        CompletableFuture<UUID>[] idFutures = executions.stream()
                .map(JobExecution::idAsync)
                .toArray(CompletableFuture[]::new);

        return allOf(concat(resultFutures, idFutures)).thenApply(unused -> {
            Map<UUID, T> results = new HashMap<>();

            for (int i = 0; i < resultFutures.length; i++) {
                results.put(idFutures[i].join(), resultFutures[i].join());
            }

            return results;
        });
    }

    private static <M, T> List<JobExecution<T>> submit(List<MapReduceJob<M, T>> runners, JobSubmitter<M, T> jobSubmitter) {
        return runners.stream()
                .map(jobSubmitter::submit)
                .collect(toList());
    }

    @Override
    public @Nullable Marshaller<R, byte[]> resultMarshaller() {
        return reduceResultMarshallerRef;
    }

    private static class SplitResult<I, M, T, R> {
        private final MapReduceTask<I, M, T, R> task;

        private final List<MapReduceJob<M, T>> runners;

        private SplitResult(MapReduceTask<I, M, T, R> task, List<MapReduceJob<M, T>> runners) {
            this.task = task;
            this.runners = runners;
        }

        private List<MapReduceJob<M, T>> runners() {
            return runners;
        }

        private MapReduceTask<I, M, T, R> task() {
            return task;
        }
    }
}
