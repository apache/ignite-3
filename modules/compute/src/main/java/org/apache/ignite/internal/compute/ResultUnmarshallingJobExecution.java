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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps {@link #resultAsync()} with marshalling.
 *
 * @param <R> Result type.
 */
public class ResultUnmarshallingJobExecution<R> implements JobExecution<R> {
    private final JobExecution<ComputeJobDataHolder> delegate;
    private final @Nullable Marshaller<R, byte[]> resultUnmarshaller;
    private final @Nullable Class<R> resultClass;
    private final HybridTimestampTracker observableTimestampTracker;

    ResultUnmarshallingJobExecution(
            JobExecution<ComputeJobDataHolder> delegate,
            @Nullable Marshaller<R, byte[]> resultUnmarshaller,
            @Nullable Class<R> resultClass,
            HybridTimestampTracker observableTimestampTracker) {
        this.delegate = delegate;
        this.resultUnmarshaller = resultUnmarshaller;
        this.resultClass = resultClass;
        this.observableTimestampTracker = observableTimestampTracker;
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return delegate.resultAsync().thenApply(
                r -> {
                    updateTimestamp(r);

                    return SharedComputeUtils.unmarshalResult(r, resultUnmarshaller, resultClass);
                });
    }

    @Override
    public CompletableFuture<@Nullable JobState> stateAsync() {
        return delegate.stateAsync();
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        return delegate.changePriorityAsync(newPriority);
    }

    @Override
    public ClusterNode node() {
        return delegate.node();
    }

    /**
     * Returns the result of the job execution along with the observable timestamp.
     *
     * @return A future that will be completed with a tuple containing the result and the observable timestamp.
     */
    public CompletableFuture<IgniteBiTuple<R, Long>> resultWithTimestampAsync() {
        return delegate.resultAsync().thenApply(r -> {
            updateTimestamp(r);

            return new IgniteBiTuple<>(
                    SharedComputeUtils.unmarshalResult(r, resultUnmarshaller, resultClass),
                    r.observableTimestamp());
        });
    }

    private void updateTimestamp(ComputeJobDataHolder r) {
        Long ts = r.observableTimestamp();
        assert ts != null : "Job result observable timestamp should not be null";
        observableTimestampTracker.update(ts);
    }
}
