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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.table.partition.HashPartition;

/** Compute job that returns a set of partition indices for the specified table passed in the context. */
public class GetPartitionsJob implements ComputeJob<String, List<Integer>> {
    @Override
    public CompletableFuture<List<Integer>> executeAsync(JobExecutionContext context, String tableName) {
        return completedFuture(context.partitions().stream()
                .filter(HashPartition.class::isInstance)
                .map(HashPartition.class::cast)
                .map(HashPartition::partitionId)
                .collect(Collectors.toList())
        );
    }
}
