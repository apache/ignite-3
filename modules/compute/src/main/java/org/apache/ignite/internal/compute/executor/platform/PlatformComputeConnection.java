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
import org.apache.ignite.internal.compute.ComputeJobDataHolder;

/**
 * Interface for executing platform (non-Java) jobs.
 */
@FunctionalInterface
public interface PlatformComputeConnection {
    /**
     * Executes a job asynchronously.
     *
     * @param deploymentUnitPaths Paths to deployment units.
     * @param jobClassName Name of the job class.
     * @param arg Arguments for the job.
     * @return A CompletableFuture that will be completed with the result of the job execution.
     */
    CompletableFuture<ComputeJobDataHolder> executeJobAsync(
            List<String> deploymentUnitPaths, String jobClassName, ComputeJobDataHolder arg);
}
