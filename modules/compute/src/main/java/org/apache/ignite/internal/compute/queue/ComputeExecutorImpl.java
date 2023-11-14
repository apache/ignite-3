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

package org.apache.ignite.internal.compute.queue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.compute.JobExecutionContextImpl;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;

/**
 * Base implementation of {@link ComputeExecutor}.
 */
public class ComputeExecutorImpl implements ComputeExecutor {
    private static final IgniteLogger LOG = Loggers.forClass(ComputeExecutorImpl.class);

    private final Ignite ignite;

    private final ComputeConfiguration configuration;

    private PriorityQueueExecutor executorService;

    public ComputeExecutorImpl(Ignite ignite, ComputeConfiguration configuration) {
        this.ignite = ignite;
        this.configuration = configuration;
    }

    @Override
    public <R> CompletableFuture<R> executeJob(ExecutionOptions options, ComputeJob<R> job, Object[] args) {
        JobExecutionContext context = new JobExecutionContextImpl(ignite);

        return executorService.submit(() -> job.execute(context, args), options.priority());
    }

    @Override
    public void start() {
        executorService = new PriorityQueueExecutor(
                configuration,
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(ignite.name(), "compute"), LOG)
        );
    }

    @Override
    public void stop() {
        executorService.shutdown();
    }

    long stopTimeoutMillis() {
        return configuration.threadPoolStopTimeoutMillis().value();
    }
}
