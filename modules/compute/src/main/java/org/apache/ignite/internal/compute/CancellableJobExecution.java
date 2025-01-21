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
import org.jetbrains.annotations.Nullable;

/**
 * {@link JobExecution} that can be cancelled.
 *
 * @param <R> Result type.
 */
public interface CancellableJobExecution<R> extends JobExecution<R> {
    /**
     * Cancels the job.
     *
     * @return The future which will be completed with {@code true} when the job is cancelled, {@code false} when the job couldn't be
     *         cancelled (if it's already completed or in the process of cancelling), or {@code null} if the job no longer exists due to
     *         exceeding the retention time limit.
     */
    CompletableFuture<@Nullable Boolean> cancelAsync();
}
