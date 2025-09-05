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

import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.compute.events.ComputeEventMetadataBuilder;
import org.apache.ignite.internal.compute.loader.JobClassLoader;
import org.apache.ignite.internal.compute.task.JobSubmitter;
import org.apache.ignite.internal.compute.task.TaskExecutionInternal;
import org.jetbrains.annotations.Nullable;

/**
 * Executor of Compute jobs.
 */
public interface ComputeExecutor {
    JobExecutionInternal<ComputeJobDataHolder> executeJob(
            ExecutionOptions options,
            String jobClassName,
            JobClassLoader classLoader,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable ComputeJobDataHolder arg
    );

    <I, M, T, R> TaskExecutionInternal<I, M, T, R> executeTask(
            JobSubmitter<M, T> jobSubmitter,
            String taskClassName,
            JobClassLoader classLoader,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable I arg
    );

    void start();

    void stop();
}
