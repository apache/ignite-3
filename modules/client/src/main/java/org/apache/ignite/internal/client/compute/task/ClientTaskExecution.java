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

package org.apache.ignite.internal.client.compute.task;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.TaskExecution;
import org.jetbrains.annotations.Nullable;

//TODO https://issues.apache.org/jira/browse/IGNITE-22124
public class ClientTaskExecution<R> implements TaskExecution<R> {
    @Override
    public CompletableFuture<R> resultAsync() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<@Nullable JobStatus> statusAsync() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<@Nullable List<JobStatus>> statusesAsync() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
