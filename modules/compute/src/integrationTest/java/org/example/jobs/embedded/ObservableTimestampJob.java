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

package org.example.jobs.embedded;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteTransactionsImpl;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;

/**
 * Compute job that returns both the per-job scoped observable timestamp and the node's global observable timestamp.
 * Used to verify that compute jobs receive the client's observable timestamp without polluting the node's global tracker.
 */
public class ObservableTimestampJob implements ComputeJob<Void, ObservableTimestampResult> {
    @Override
    public CompletableFuture<ObservableTimestampResult> executeAsync(JobExecutionContext context, Void arg) {
        long perJobTs = unwrapIgniteTransactionsImpl(context.ignite().transactions()).observableTimestampTracker().getLong();
        long nodeGlobalTs = unwrapIgniteImpl(context.ignite()).observableTimeTracker().getLong();

        return completedFuture(new ObservableTimestampResult(perJobTs, nodeGlobalTs));
    }
}
