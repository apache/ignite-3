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

import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.TaskExecution;
import org.apache.ignite.internal.compute.task.AntiHijackTaskExecution;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Wrapper around {@link IgniteCompute} that adds protection against thread hijacking by users.
 */
public class AntiHijackIgniteCompute implements IgniteCompute, Wrapper {
    private final IgniteCompute compute;
    private final Executor asyncContinuationExecutor;

    /**
     * Constructor.
     */
    public AntiHijackIgniteCompute(IgniteCompute compute, Executor asyncContinuationExecutor) {
        this.compute = compute;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public <R> JobExecution<R> submit(Set<ClusterNode> nodes, JobDescriptor descriptor, Object... args) {
        List<DeploymentUnit> units = descriptor.units();
        String jobClassName = descriptor.jobClassName();
        JobExecutionOptions options = descriptor.options();
        return preventThreadHijack(compute.submit(nodes, JobDescriptor.builder()
                .jobClassName(jobClassName)
                .units(units)
                .options(options)
                .build(), args));
    }

    @Override
    public <R> R execute(Set<ClusterNode> nodes, JobDescriptor descriptor, Object... args) {
        return compute.execute(nodes, descriptor, args);
    }

    @Override
    public <R> JobExecution<R> submitColocated(
            String tableName,
            Tuple key,
            JobDescriptor descriptor,
            Object... args
    ) {
        return preventThreadHijack(
                compute.submitColocated(tableName, key, descriptor.units(), descriptor.jobClassName(), descriptor.options(), args));
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
        return preventThreadHijack(compute.submitColocated(tableName, key, keyMapper, units, jobClassName, options, args));
    }

    @Override
    public <R> R executeColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return compute.executeColocated(tableName, key, units, jobClassName, options, args);
    }

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
        return compute.executeColocated(tableName, key, keyMapper, units, jobClassName, options, args);
    }

    @Override
    public <R> Map<ClusterNode, JobExecution<R>> submitBroadcast(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        Map<ClusterNode, JobExecution<R>> results = compute.submitBroadcast(nodes, units, jobClassName, options, args);

        return results.entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> preventThreadHijack(entry.getValue())));
    }

    @Override
    public <R> TaskExecution<R> submitMapReduce(List<DeploymentUnit> units, String taskClassName, Object... args) {
        return new AntiHijackTaskExecution<>(compute.submitMapReduce(units, taskClassName, args), asyncContinuationExecutor);
    }

    @Override
    public <R> R executeMapReduce(List<DeploymentUnit> units, String taskClassName, Object... args) {
        return compute.executeMapReduce(units, taskClassName, args);
    }

    private <R> JobExecution<R> preventThreadHijack(JobExecution<R> execution) {
        return new AntiHijackJobExecution<>(execution, asyncContinuationExecutor);
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(compute);
    }
}
