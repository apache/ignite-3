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

package org.apache.ignite.client.fakes;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.compute.JobState.COMPLETED;
import static org.apache.ignite.compute.JobState.EXECUTING;
import static org.apache.ignite.compute.JobState.FAILED;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.TaskExecution;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.compute.JobExecutionContextImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/**
 * Fake {@link IgniteCompute}.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class FakeCompute implements IgniteComputeInternal {
    public static final String GET_UNITS = "get-units";

    public static volatile @Nullable CompletableFuture future;

    public static volatile @Nullable RuntimeException err;

    private final Map<UUID, JobStatus> statuses = new ConcurrentHashMap<>();

    public static volatile CountDownLatch latch = new CountDownLatch(0);

    private final String nodeName;

    private final Ignite ignite;

    public FakeCompute(String nodeName, Ignite ignite) {
        this.nodeName = nodeName;
        this.ignite = ignite;
    }

    @Override
    public <R> JobExecution<R> submit(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args) {
        return executeAsyncWithFailover(nodes, units, jobClassName, options, args);
    }

    @Override
    public <R> JobExecution<R> executeAsyncWithFailover(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args) {
        if (Objects.equals(jobClassName, GET_UNITS)) {
            String unitString = units.stream().map(DeploymentUnit::render).collect(Collectors.joining(","));
            return completedExecution((R) unitString);
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (err != null) {
            throw err;
        }

        if (jobClassName.startsWith("org.apache.ignite")) {
            Class<ComputeJob<Object>> jobClass = ComputeUtils.jobClass(this.getClass().getClassLoader(), jobClassName);
            ComputeJob<Object> job = ComputeUtils.instantiateJob(jobClass);
            Object jobRes = job.execute(new JobExecutionContextImpl(ignite, new AtomicBoolean()), args);

            return jobExecution(completedFuture((R) jobRes));
        }

        return jobExecution(future != null ? future : completedFuture((R) nodeName));
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<JobExecution<R>> submitColocatedInternal(TableViewInternal table, Tuple key, List<DeploymentUnit> units,
            String jobClassName, JobExecutionOptions options, Object[] args) {
        return completedFuture(jobExecution(future != null ? future : completedFuture((R) nodeName)));
    }

    /** {@inheritDoc} */
    @Override
    public <R> R execute(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return sync(executeAsync(nodes, units, jobClassName, options, args));
    }

    @Override
    public <R> JobExecution<R> submitColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return jobExecution(future != null ? future : completedFuture((R) nodeName));
    }

    @Override
    public <K, R> JobExecution<R> submitColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return jobExecution(future != null ? future : completedFuture((R) nodeName));
    }

    /** {@inheritDoc} */
    @Override
    public <R> R executeColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return sync(executeColocatedAsync(tableName, key, units, jobClassName, options, args));
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> R executeColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return sync(executeColocatedAsync(tableName, key, keyMapper, units, jobClassName, options, args));
    }

    @Override
    public <R> Map<ClusterNode, JobExecution<R>> submitBroadcast(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return null;
    }

    @Override
    public <R> TaskExecution<R> submitMapReduce(List<DeploymentUnit> units, String taskClassName, Object... args) {
        return taskExecution(future != null ? future : completedFuture((R) nodeName));
    }

    @Override
    public <R> R executeMapReduce(List<DeploymentUnit> units, String taskClassName, Object... args) {
        return sync(executeMapReduceAsync(units, taskClassName, args));
    }

    private <R> JobExecution<R> completedExecution(R result) {
        return jobExecution(completedFuture(result));
    }

    private <R> JobExecution<R> jobExecution(CompletableFuture<R> result) {
        UUID jobId = UUID.randomUUID();

        JobStatus status = JobStatus.builder()
                .id(jobId)
                .state(EXECUTING)
                .createTime(Instant.now())
                .startTime(Instant.now())
                .build();
        statuses.put(jobId, status);

        result.whenComplete((r, throwable) -> {
            JobState state = throwable != null ? FAILED : COMPLETED;
            JobStatus newStatus = status.toBuilder().id(jobId).state(state).finishTime(Instant.now()).build();
            statuses.put(jobId, newStatus);
        });
        return new JobExecution<>() {
            @Override
            public CompletableFuture<R> resultAsync() {
                return result;
            }

            @Override
            public CompletableFuture<@Nullable JobStatus> statusAsync() {
                return completedFuture(statuses.get(jobId));
            }

            @Override
            public CompletableFuture<@Nullable Boolean> cancelAsync() {
                return trueCompletedFuture();
            }

            @Override
            public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
                return trueCompletedFuture();
            }
        };
    }

    private <R> TaskExecution<R> taskExecution(CompletableFuture<R> result) {
        BiFunction<UUID, JobState, JobStatus> toStatus = (id, jobState) ->
                JobStatus.builder()
                        .id(id)
                        .state(jobState)
                        .createTime(Instant.now())
                        .startTime(Instant.now())
                        .build();

        UUID jobId = UUID.randomUUID();
        UUID subJobId1 = UUID.randomUUID();
        UUID subJobId2 = UUID.randomUUID();

        statuses.put(jobId, toStatus.apply(jobId, EXECUTING));
        statuses.put(subJobId1, toStatus.apply(subJobId1, EXECUTING));
        statuses.put(subJobId2, toStatus.apply(subJobId2, EXECUTING));

        result.whenComplete((r, throwable) -> {
            JobState state = throwable != null ? FAILED : COMPLETED;

            statuses.put(jobId, toStatus.apply(jobId, state));
            statuses.put(subJobId1, toStatus.apply(subJobId1, state));
            statuses.put(subJobId2, toStatus.apply(subJobId2, state));
        });

        return new TaskExecution<>() {
            @Override
            public CompletableFuture<R> resultAsync() {
                return result;
            }

            @Override
            public CompletableFuture<@Nullable JobStatus> statusAsync() {
                return completedFuture(statuses.get(jobId));
            }

            @Override
            public CompletableFuture<List<@Nullable JobStatus>> statusesAsync() {
                return completedFuture(List.of(statuses.get(subJobId1), statuses.get(subJobId2)));
            }

            @Override
            public CompletableFuture<@Nullable Boolean> cancelAsync() {
                return trueCompletedFuture();
            }

            @Override
            public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
                return trueCompletedFuture();
            }
        };
    }

    @Override
    public CompletableFuture<Collection<JobStatus>> statusesAsync() {
        return completedFuture(statuses.values());
    }

    @Override
    public CompletableFuture<@Nullable JobStatus> statusAsync(UUID jobId) {
        return completedFuture(statuses.get(jobId));
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync(UUID jobId) {
        return trueCompletedFuture();
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(UUID jobId, int newPriority) {
        return trueCompletedFuture();
    }

    private static <R> R sync(CompletableFuture<R> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
    }
}
