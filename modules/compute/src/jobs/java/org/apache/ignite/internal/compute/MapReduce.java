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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.deployment.DeploymentUnit;

/** Map reduce task which runs a {@link GetNodeNameJob} on each node and computes a sum of length of all node names. */
public class MapReduce implements MapReduceTask<Integer> {
    @Override
    public CompletableFuture<List<MapReduceJob>> splitAsync(TaskExecutionContext taskContext, Object... args) {
        List<DeploymentUnit> deploymentUnits = (List<DeploymentUnit>) args[0];

        return taskContext.ignite().clusterNodesAsync().thenApply(nodes -> nodes.stream().map(node ->
                MapReduceJob.builder()
                        .jobDescriptor(JobDescriptor.builder(GetNodeNameJob.class)
                                .units(deploymentUnits)
                                .options(JobExecutionOptions.builder()
                                    .maxRetries(10)
                                    .priority(Integer.MAX_VALUE)
                                    .build())
                                .build())
                        .nodes(Set.of(node))
                        .build()
        ).collect(toList()));
    }

    @Override
    public CompletableFuture<Integer> reduceAsync(TaskExecutionContext taskContext, Map<UUID, ?> results) {
        return completedFuture(results.values().stream()
                .map(String.class::cast)
                .map(String::length)
                .reduce(Integer::sum)
                .orElseThrow());
    }
}
