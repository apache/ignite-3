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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/**
 * Fake {@link IgniteCompute}.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class FakeCompute implements IgniteCompute {
    public static final String GET_UNITS = "get-units";

    public static volatile @Nullable CompletableFuture future;

    public static volatile CountDownLatch latch = new CountDownLatch(0);

    private final String nodeName;

    public FakeCompute(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public <R> CompletableFuture<R> executeAsync(Set<ClusterNode> nodes, List<DeploymentUnit> units, String jobClassName, Object... args) {
        if (Objects.equals(jobClassName, GET_UNITS)) {
            String unitString = units.stream().map(DeploymentUnit::render).collect(Collectors.joining(","));
            return CompletableFuture.completedFuture((R) unitString);
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return future != null ? future : CompletableFuture.completedFuture((R) nodeName);
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
            return this.<R>executeAsync(nodes, units, jobClassName, args).join();
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
    }

    @Override
    public <R> CompletableFuture<R> executeColocatedAsync(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return future != null ? future : CompletableFuture.completedFuture((R) nodeName);
    }

    @Override
    public <K, R> CompletableFuture<R> executeColocatedAsync(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return future != null ? future : CompletableFuture.completedFuture((R) nodeName);
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
            return this.<R>executeColocatedAsync(tableName, key, units, jobClassName, args).join();
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
            return this.<K, R>executeColocatedAsync(tableName, key, keyMapper, units, jobClassName, args).join();
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
    }

    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcastAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return null;
    }
}
