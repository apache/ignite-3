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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
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

    public static volatile @Nullable JobStatus status;

    public static volatile CountDownLatch latch = new CountDownLatch(0);

    private final String nodeName;

    public FakeCompute(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public <R> JobExecution<R> executeAsync(Set<ClusterNode> nodes, List<DeploymentUnit> units, String jobClassName, Object... args) {
        initStatus();

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
    public <R> R execute(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        try {
            return this.<R>executeAsync(nodes, units, jobClassName, args).resultAsync().join();
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
    }

    @Override
    public <R> JobExecution<R> executeColocatedAsync(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        initStatus();
        return jobExecution(future != null ? future : completedFuture((R) nodeName));
    }

    @Override
    public <K, R> JobExecution<R> executeColocatedAsync(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        initStatus();
        return jobExecution(future != null ? future : completedFuture((R) nodeName));
    }

    /** {@inheritDoc} */
    @Override
    public <R> R executeColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        try {
            return this.<R>executeColocatedAsync(tableName, key, units, jobClassName, args).resultAsync().join();
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
            Object... args
    ) {
        try {
            return this.<K, R>executeColocatedAsync(tableName, key, keyMapper, units, jobClassName, args).resultAsync().join();
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
    }

    @Override
    public <R> Map<ClusterNode, JobExecution<R>> broadcastAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return null;
    }

    private static <R> JobExecution<R> completedExecution(R result) {
        return jobExecution(completedFuture(result));
    }

    private static void initStatus() {
        status = JobStatus.builder()
                .id(UUID.randomUUID())
                .state(EXECUTING)
                .createTime(Instant.now())
                .startTime(Instant.now())
                .build();
    }

    private static <R> JobExecution<R> jobExecution(CompletableFuture<R> result) {
        result.whenComplete((r, throwable) -> {
            JobState state = throwable != null ? FAILED : COMPLETED;
            status = status.toBuilder().state(state).finishTime(Instant.now()).build();
        });
        return new JobExecution<>() {
            @Override
            public CompletableFuture<R> resultAsync() {
                return result;
            }

            @Override
            public CompletableFuture<JobStatus> statusAsync() {
                return completedFuture(status);
            }

            @Override
            public CompletableFuture<Boolean> cancelAsync() {
                return trueCompletedFuture();
            }
        };
    }

    @Override
    public CompletableFuture<JobStatus> statusAsync(UUID jobId) {
        return completedFuture(status);
    }

    @Override
    public CompletableFuture<Boolean> cancelAsync(UUID jobId) {
        return trueCompletedFuture();
    }
}
