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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/**
 * Fake {@link IgniteCompute}.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class FakeCompute implements IgniteCompute {
    public static volatile @Nullable CompletableFuture future;

    private final String nodeName;

    public FakeCompute(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass, Object... args) {
        return future != null ? future : CompletableFuture.completedFuture((R) nodeName);
    }

    @Override
    public <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, String jobClassName, Object... args) {
        return future != null ? future : CompletableFuture.completedFuture((R) nodeName);
    }

    @Override
    public <R> CompletableFuture<R> executeColocated(String tableName, Tuple key, Class<? extends ComputeJob<R>> jobClass, Object... args) {
        return future != null ? future : CompletableFuture.completedFuture((R) nodeName);
    }

    @Override
    public <K, R> CompletableFuture<R> executeColocated(String tableName, K key, Mapper<K> keyMapper,
            Class<? extends ComputeJob<R>> jobClass, Object... args) {
        return future != null ? future : CompletableFuture.completedFuture((R) nodeName);
    }

    @Override
    public <R> CompletableFuture<R> executeColocated(String tableName, Tuple key, String jobClassName, Object... args) {
        return future != null ? future : CompletableFuture.completedFuture((R) nodeName);
    }

    @Override
    public <K, R> CompletableFuture<R> executeColocated(String tableName, K key, Mapper<K> keyMapper, String jobClassName, Object... args) {
        return future != null ? future : CompletableFuture.completedFuture((R) nodeName);
    }

    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass,
            Object... args) {
        return null;
    }

    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, String jobClassName, Object... args) {
        return null;
    }
}
