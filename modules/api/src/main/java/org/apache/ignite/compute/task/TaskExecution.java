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

package org.apache.ignite.compute.task;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.jetbrains.annotations.Nullable;

/**
 * Compute task control object. Methods inherited from the {@link JobExecution} allows control of the task coordination job.
 *
 * @param <R> Task result type.
 */
public interface TaskExecution<R> extends JobExecution<R> {
    /**
     * Returns a collection of states of the jobs which are executing under this task. The resulting future is completed only after the
     * jobs are submitted for execution. The list could contain {@code null} values if the time for retaining job state has been exceeded.
     *
     * @return A list of current states of the jobs.
     */
    CompletableFuture<List<@Nullable JobState>> statesAsync();

    /**
     * Returns a collection of ids of the jobs which are executing under this task. The resulting future is completed only after the
     * jobs are submitted for execution. The list could contain {@code null} values if the time for retaining job state has been exceeded.
     *
     * @return A list of ids of the jobs.
     */
    default CompletableFuture<List<@Nullable UUID>> idsAsync() {
        return statesAsync().thenApply(states -> states.stream()
                .map(state -> state != null ? state.id() : null)
                .collect(Collectors.toList()));
    }
}
