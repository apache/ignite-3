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
import java.util.stream.Collectors;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
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

    public FakeCompute(String nodeName) {
        this.nodeName = nodeName;
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
        try {
            return this.<R>submit(nodes, units, jobClassName, options, args).resultAsync().join();
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
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
        try {
            return this.<R>submitColocated(tableName, key, units, jobClassName, options, args).resultAsync().join();
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
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
        try {
            return this.<K, R>submitColocated(tableName, key, keyMapper, units, jobClassName, options, args).resultAsync().join();
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
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
            JobStatus newStatus = status.toBuilder().state(state).finishTime(Instant.now()).build();
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
}
