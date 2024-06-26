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

import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.compute.loader.JobClassLoader;
import org.apache.ignite.internal.compute.task.JobSubmitter;
import org.apache.ignite.internal.compute.task.TaskExecutionInternal;

/**
 * Executor of Compute jobs.
 */
public interface ComputeExecutor {
    <T, R> JobExecutionInternal executeJob(
            ExecutionOptions options,
            Class<? extends ComputeJob<T, R>> jobClass,
            JobClassLoader classLoader,
            T input);

    <T, R> TaskExecutionInternal<T, R> executeTask(
            JobSubmitter jobSubmitter,
            Class<? extends MapReduceTask<T, R>> taskClass,
            T input
    );

    void start();

    void stop();
}
