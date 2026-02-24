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
import static org.apache.ignite.internal.compute.ComputeUtils.getTaskSplitArgumentType;
import static org.apache.ignite.internal.compute.ComputeUtils.instantiateTask;
import static org.apache.ignite.internal.compute.ComputeUtils.unmarshalOrNotIfNull;
import static org.apache.ignite.internal.compute.events.ComputeEventsFactory.logEvent;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_CANCELED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_COMPLETED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_EXECUTING;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_FAILED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_TASK_QUEUED;
import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;
import static org.apache.ignite.internal.util.ArrayUtils.concat;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.TaskState;
import org.apache.ignite.compute.TaskStatus;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.internal.compute.CancellableTaskExecution;
import org.apache.ignite.internal.compute.HybridTimestampProvider;
import org.apache.ignite.internal.compute.MarshallerProvider;
import org.apache.ignite.internal.compute.ResultUnmarshallingJobExecution;
import org.apache.ignite.internal.compute.TaskStateImpl;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata;
import org.apache.ignite.internal.compute.events.ComputeEventMetadataBuilder;
import org.apache.ignite.internal.compute.queue.PriorityQueueExecutor;
import org.apache.ignite.internal.compute.queue.QueueExecution;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.CancelHandle;
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
public class TaskExecutionInternal<I, M, T, R> implements CancellableTaskExecution<R>, MarshallerProvider<R>, HybridTimestampProvider {
    private static final IgniteLogger LOG = Loggers.forClass(TaskExecutionInternal.class);

    private final QueueExecution<SplitResult<I, M, T, R>> splitExecution;

    private final CompletableFuture<List<JobExecution<T>>> executionsFuture;

    private final CompletableFuture<IgniteBiTuple<Map<UUID, T>, Long>> resultsFuture;

    private final CompletableFuture<QueueExecution<R>> reduceExecutionFuture;

    private final AtomicReference<TaskState> reduceFailedState = new AtomicReference<>();

    private final CancelHandle cancelHandle = CancelHandle.create();

    private final EventLog eventLog;

    private final AtomicBoolean isCancelled;

    private final UUID taskId = UUID.randomUUID();

    private final ComputeEventMetadata eventMetadata;

    private volatile @Nullable Marshaller<R, byte[]> reduceResultMarshallerRef;

    /**
     * Construct an execution object and starts executing.
     *
     * @param executorService Compute jobs executor.
     * @param eventLog Event log.
     * @param jobSubmitter Compute jobs submitter.
     * @param taskClass Map reduce task class.
     * @param context Task execution context.
     * @param isCancelled Flag which is passed to the execution context so that the task can check it for cancellation request.
     * @param metadataBuilder Event metadata builder.
     * @param arg Task argument.
     */
    public TaskExecutionInternal(
            PriorityQueueExecutor executorService,
            EventLog eventLog,
            JobSubmitter<M, T> jobSubmitter,
            Class<? extends MapReduceTask<I, M, T, R>> taskClass,
            TaskExecutionContext context,
            AtomicBoolean isCancelled,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable I arg
    ) {
        this.eventLog = eventLog;
        this.isCancelled = isCancelled;
        eventMetadata = metadataBuilder.taskId(taskId).build();

        LOG.debug("Executing task {}", taskClass.getName());

        logEvent(eventLog, COMPUTE_TASK_QUEUED, eventMetadata);

        splitExecution = executorService.submit(
                () -> {
                    logEvent(eventLog, COMPUTE_TASK_EXECUTING, eventMetadata);

                    MapReduceTask<I, M, T, R> task = instantiateTask(taskClass);

                    reduceResultMarshallerRef = task.reduceJobResultMarshaller();

                    Class<?> splitArgumentType = getTaskSplitArgumentType(taskClass);
                    I input = unmarshalOrNotIfNull(task.splitJobInputMarshaller(), arg, splitArgumentType, taskClass.getClassLoader());
                    return task.splitAsync(context, input).thenApply(jobs -> new SplitResult<>(task, jobs));
                },

                Integer.MAX_VALUE,
                0
        );

        executionsFuture = splitExecution.resultAsync().thenCompose(splitResult -> {
            List<MapReduceJob<M, T>> runners = splitResult.runners();
            LOG.debug("Submitting {} jobs for {}", runners.size(), taskClass.getName());
            return jobSubmitter.submit(runners, metadataBuilder, cancelHandle.token());
        });

        resultsFuture = executionsFuture.thenCompose(TaskExecutionInternal::resultsAsync);

        reduceExecutionFuture = resultsFuture.thenApply(results -> {
            LOG.debug("Running reduce job for {}", taskClass.getName());

            // This future is already finished
            MapReduceTask<I, M, T, R> task = splitExecution.resultAsync().thenApply(SplitResult::task).join();

            Map<UUID, T> resMap = results.get1();
            assert resMap != null : "Results map should not be null";

            return executorService.submit(
                    () -> task.reduceAsync(context, resMap),
                    Integer.MAX_VALUE,
                    0
            );
        }).whenComplete(this::captureReduceExecution);
    }

    private void captureReduceExecution(QueueExecution<R> reduceExecution, Throwable throwable) {
        if (throwable != null) {
            captureReduceSubmitFailure();
        } else {
            handleReduceResult(reduceExecution);
        }
    }

    private void captureReduceSubmitFailure() {
        // Capture the reduce submit failure reason and time.
        TaskStatus status = isCancelled.get() ? CANCELED : FAILED;

        logEvent(eventLog, status == CANCELED ? COMPUTE_TASK_CANCELED : COMPUTE_TASK_FAILED, eventMetadata);

        JobState state = splitExecution.state();
        if (state != null) {
            reduceFailedState.set(
                    TaskStateImpl.toBuilder(state)
                            .id(taskId)
                            .status(status)
                            .finishTime(Instant.now())
                            .build()
            );
        }
    }

    private void handleReduceResult(QueueExecution<R> reduceExecution) {
        reduceExecution.resultAsync().whenComplete((result, throwable) -> {
            if (result != null) {
                logEvent(eventLog, COMPUTE_TASK_COMPLETED, eventMetadata);
            } else {
                JobState reduceState = reduceExecution.state();
                // The state should never be null since it was just submitted, but check just in case.
                if (reduceState != null) {
                    IgniteEventType type = reduceState.status() == JobStatus.FAILED ? COMPUTE_TASK_FAILED : COMPUTE_TASK_CANCELED;
                    logEvent(eventLog, type, eventMetadata);
                }
            }
        });
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
            return completedFuture(TaskStateImpl.toBuilder(splitState).id(taskId).build());
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
                            .id(taskId)
                            .createTime(splitState.createTime())
                            .startTime(splitState.startTime())
                            .build();
                }
                return reduceFailedState.get();
            });
        }

        // At this point split is complete but reduce job is not submitted yet.
        return completedFuture(TaskStateImpl.toBuilder(splitState)
                .id(taskId)
                .status(EXECUTING)
                .finishTime(null)
                .build());
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        if (!isCancelled.compareAndSet(false, true)) {
            return falseCompletedFuture();
        }

        // If the split job is not complete.
        if (splitExecution.cancel()) {
            // The split job cancelled, but since it's not a direct chain of futures, we should cancel the next future in chain manually.
            executionsFuture.cancel(true);
            return trueCompletedFuture();
        }

        // This means we didn't submit any jobs yet.
        if (executionsFuture.cancel(true)) {
            return trueCompletedFuture();
        }

        // Split job was complete, results future was running, but not complete yet.
        if (resultsFuture.cancel(true)) {
            return executionsFuture.thenCompose(unused -> cancelHandle.cancelAsync())
                    .thenApply(unused -> true);
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

    private static <T> CompletableFuture<IgniteBiTuple<Map<UUID, T>, Long>> resultsAsync(List<JobExecution<T>> executions) {
        CompletableFuture<IgniteBiTuple<T, Long>>[] resultFutures = executions.stream()
                .map(j -> ((ResultUnmarshallingJobExecution<IgniteBiTuple<T, Long>>) j).resultWithTimestampAsync())
                .toArray(CompletableFuture[]::new);

        CompletableFuture<UUID>[] idFutures = executions.stream()
                .map(JobExecution::idAsync)
                .toArray(CompletableFuture[]::new);

        return allOf(concat(resultFutures, idFutures)).thenApply(unused -> {
            Map<UUID, T> results = new LinkedHashMap<>();
            long timestamp = NULL_HYBRID_TIMESTAMP;

            for (int i = 0; i < resultFutures.length; i++) {
                IgniteBiTuple<T, Long> jobRes = resultFutures[i].join();
                results.put(idFutures[i].join(), jobRes.get1());

                Long jobTs = jobRes.get2();
                assert jobTs != null : "Job result timestamp should not be null";

                timestamp = Math.max(timestamp, jobTs);
            }

            return new IgniteBiTuple<>(results, timestamp);
        });
    }

    @Override
    public @Nullable Marshaller<R, byte[]> resultMarshaller() {
        return reduceResultMarshallerRef;
    }

    @Override
    public boolean marshalResult() {
        // Not needed because split/reduce jobs always run on the client handler node
        return false;
    }

    @Override
    public long hybridTimestamp() {
        if (resultsFuture.isCompletedExceptionally()) {
            return NULL_HYBRID_TIMESTAMP;
        }

        IgniteBiTuple<Map<UUID, T>, Long> res = resultsFuture.getNow(null);

        if (res == null) {
            throw new IllegalStateException("Task execution is not complete yet, cannot get hybrid timestamp.");
        }

        assert res.get2() != null : "Task result timestamp should not be null";

        return res.get2();
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
