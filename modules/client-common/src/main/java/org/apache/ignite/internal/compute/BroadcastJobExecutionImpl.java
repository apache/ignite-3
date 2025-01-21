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

import static java.util.concurrent.CompletableFuture.allOf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.JobExecution;

/**
 * {@link BroadcastExecution} implementation. Contains a collection of individual executions.
 *
 * @param <R> Result type.
 */
public class BroadcastJobExecutionImpl<R> implements BroadcastExecution<R> {
    private final Collection<JobExecution<R>> executions;

    public BroadcastJobExecutionImpl(Collection<JobExecution<R>> executions) {
        this.executions = executions;
    }

    @Override
    public Collection<JobExecution<R>> executions() {
        return List.copyOf(executions);
    }

    @Override
    public CompletableFuture<Collection<R>> resultsAsync() {
        CompletableFuture<R>[] futures = executions.stream()
                .map(JobExecution::resultAsync)
                .toArray(CompletableFuture[]::new);

        return allOf(futures).thenApply(ignored -> {
            ArrayList<R> result = new ArrayList<>(futures.length);

            for (CompletableFuture<R> future : futures) {
                result.add(future.join());
            }

            return result;
        });
    }
}
