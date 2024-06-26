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

package org.apache.ignite.internal.compute.executor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.compute.queue.QueueExecution;
import org.jetbrains.annotations.Nullable;

/**
 * Internal job execution object.
 */
public class JobExecutionInternal {
    private final QueueExecution<?> execution;

    private final AtomicBoolean isInterrupted;

    /**
     * Constructor.
     *
     * @param execution Internal execution state.
     * @param isInterrupted Flag which is passed to the execution context so that the job can check it for cancellation request.
     */
    JobExecutionInternal(QueueExecution<?> execution, AtomicBoolean isInterrupted) {
        this.execution = execution;
        this.isInterrupted = isInterrupted;
    }

    public CompletableFuture<?> resultAsync() {
        return execution.resultAsync();
    }

    @Nullable
    public JobState state() {
        return execution.state();
    }

    /**
     * Cancel job execution.
     *
     * @return {@code true} if job was successfully cancelled.
     */
    public boolean cancel() {
        isInterrupted.set(true);
        return execution.cancel();
    }

    /**
     * Change priority of job execution.
     *
     * @param newPriority new priority.
     * @return {@code true} if priority was successfully changed.
     */
    public boolean changePriority(int newPriority) {
        return execution.changePriority(newPriority);
    }
}
