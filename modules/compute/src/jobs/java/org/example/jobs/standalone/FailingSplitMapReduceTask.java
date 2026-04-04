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

package org.example.jobs.standalone;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;

/** Map reduce task that throws an exception from the splitAsync. */
public class FailingSplitMapReduceTask implements MapReduceTask<Void, Void, Void, Void> {
    @Override
    public CompletableFuture<List<MapReduceJob<Void, Void>>> splitAsync(TaskExecutionContext taskContext, Void input) {
        throw new RuntimeException();
    }

    @Override
    public CompletableFuture<Void> reduceAsync(TaskExecutionContext taskContext, Map<UUID, Void> results) {
        return nullCompletedFuture();
    }
}
