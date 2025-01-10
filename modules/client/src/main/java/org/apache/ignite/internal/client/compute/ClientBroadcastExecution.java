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

package org.apache.ignite.internal.client.compute;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.network.ClusterNode;


/**
 * Client job execution implementation.
 */
class ClientBroadcastExecution<R> implements BroadcastExecution<R> {
    private final Map<ClusterNode, JobExecution<R>> executions;

    ClientBroadcastExecution(Map<ClusterNode, JobExecution<R>> executions) {
        this.executions = executions;
    }

    @Override
    public Map<ClusterNode, JobExecution<R>> executions() {
        return Map.copyOf(executions);
    }

    @Override
    public CompletableFuture<Collection<R>> resultsAsync() {
        CompletableFuture<R>[] futures = executions.values().stream()
                .map(JobExecution::resultAsync)
                .toArray(CompletableFuture[]::new);

        return allOf(futures).thenApply(ignored -> {
            ArrayList<R> result = new ArrayList<>();

            for (CompletableFuture<R> future : futures) {
                result.add(future.join());
            }

            return result;
        });
    }

    private <T> CompletableFuture<Map<ClusterNode, T>> mapExecutions(Function<JobExecution<R>, CompletableFuture<T>> mapping) {
        Map<ClusterNode, CompletableFuture<T>> futures = executions.entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> mapping.apply(entry.getValue())));

        return allOf(futures.values().toArray(CompletableFuture[]::new))
                .thenApply(ignored -> {
                            Map<ClusterNode, T> map = new HashMap<>();

                            for (Entry<ClusterNode, CompletableFuture<T>> entry : futures.entrySet()) {
                                map.put(entry.getKey(), entry.getValue().join());
                            }

                            return map;
                        }
                );
    }
}
