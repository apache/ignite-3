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

package org.apache.ignite.internal.compute.executor.platform;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for executing platform (non-Java) jobs.
 */
public interface PlatformComputeConnection {
    /**
     * Executes a job asynchronously.
     *
     * @param jobId Job id (for cancellation).
     * @param jobClassName Name of the job class.
     * @param ctx Job execution context.
     * @param arg Arguments for the job.
     * @return A CompletableFuture that will be completed with the result of the job execution.
     */
    CompletableFuture<ComputeJobDataHolder> executeJobAsync(
            long jobId,
            String jobClassName,
            JobExecutionContext ctx,
            @Nullable ComputeJobDataHolder arg
    );

    /**
     * Cancels a job started by {@link #executeJobAsync}.
     *
     * @param jobId Job id.
     * @return True if the job was cancelled, false if it was already completed or cancelled.
     */
    CompletableFuture<Boolean> cancelJobAsync(long jobId);

    /**
     * Un-deploys deployment units asynchronously.
     *
     * @param deploymentUnitPaths Paths to deployment units.
     * @return True if the undeployment was successful, false if the units were not deployed or already undeployed.
     */
    CompletableFuture<Boolean> undeployUnitsAsync(List<String> deploymentUnitPaths);

    /**
     * Closes the connection.
     */
    void close();

    /**
     * Checks if the connection is closed.
     *
     * @return True if the connection is closed, false otherwise.
     */
    boolean isActive();
}
