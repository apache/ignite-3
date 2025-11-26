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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;

/** Map reduce task that throws an exception from the reduceAsync. */
public class FailingReduceMapReduceTask implements MapReduceTask<Void, Void, String, Void> {
    @Override
    public CompletableFuture<List<MapReduceJob<Void, String>>> splitAsync(TaskExecutionContext taskContext, Void input) {
        return completedFuture(List.of(
                MapReduceJob.<Void, String>builder()
                        .jobDescriptor(JobDescriptor.builder(GetNodeNameJob.class).build())
                        .nodes(taskContext.ignite().cluster().nodes())
                        .build()
        ));
    }

    @Override
    public CompletableFuture<Void> reduceAsync(TaskExecutionContext taskContext, Map<UUID, String> results) {
        throw new RuntimeException();
    }
}
