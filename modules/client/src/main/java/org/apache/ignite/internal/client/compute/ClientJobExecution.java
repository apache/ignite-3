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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;


/**
 * Client job execution implementation.
 */
class ClientJobExecution<R> implements JobExecution<R> {
    private final CompletableFuture<R> result;

    ClientJobExecution(CompletableFuture<R> result) {
        this.result = result;
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return result;
    }

    @Override
    public CompletableFuture<JobStatus> statusAsync() {
        // TODO https://issues.apache.org/jira/browse/IGNITE-21148
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> cancelAsync() {
        // TODO https://issues.apache.org/jira/browse/IGNITE-21148
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> changePriorityAsync(int newPriority) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-21148
        return nullCompletedFuture();
    }
}
