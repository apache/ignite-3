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

import java.util.List;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.compute.events.ComputeEventMetadataBuilder;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Captures the context of a job execution.
 */
public class ExecutionContext {
    private final ExecutionOptions options;

    private final List<DeploymentUnit> units;

    private final String jobClassName;

    private final ComputeEventMetadataBuilder metadataBuilder;

    private final ComputeJobDataHolder arg;

    /**
     * Creates new execution context.
     *
     * @param options Job execution options.
     * @param units Deployment units which will be loaded for execution.
     * @param jobClassName Name of the job class.
     * @param metadataBuilder Event metadata builder.
     * @param arg Job argument.
     */
    public ExecutionContext(
            ExecutionOptions options,
            List<DeploymentUnit> units,
            String jobClassName,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable ComputeJobDataHolder arg
    ) {
        this.options = options;
        this.units = units;
        this.jobClassName = jobClassName;
        this.metadataBuilder = metadataBuilder;
        this.arg = arg;
    }

    /**
     * Creates new execution context.
     *
     * @param jobExecutionOptions Job execution options.
     * @param units Deployment units which will be loaded for execution.
     * @param jobClassName Name of the job class.
     * @param metadataBuilder Event metadata builder.
     * @param arg Job argument.
     */
    public ExecutionContext(
            JobExecutionOptions jobExecutionOptions,
            List<DeploymentUnit> units,
            String jobClassName,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable ComputeJobDataHolder arg
    ) {
        this(ExecutionOptions.from(jobExecutionOptions), units, jobClassName, metadataBuilder, arg);
    }

    /**
     * Creates new execution context. Takes execution options, deployment units and job class name from a job descriptor.
     *
     * @param descriptor Job descriptor.
     * @param metadataBuilder Event metadata builder.
     * @param arg Job argument.
     */
    public <T, R> ExecutionContext(
            JobDescriptor<T, R> descriptor,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable ComputeJobDataHolder arg
    ) {
        this(descriptor.options(), descriptor.units(), descriptor.jobClassName(), metadataBuilder, arg);
    }

    public ExecutionOptions options() {
        return options;
    }

    public List<DeploymentUnit> units() {
        return units;
    }

    public String jobClassName() {
        return jobClassName;
    }

    public ComputeEventMetadataBuilder metadataBuilder() {
        return metadataBuilder;
    }

    @Nullable
    public ComputeJobDataHolder arg() {
        return arg;
    }

    public long observableTimestamp() {
        if (arg == null) {
            return HybridTimestamp.NULL_HYBRID_TIMESTAMP;
        }

        Long ts = arg.observableTimestamp();

        return ts == null
                ? HybridTimestamp.NULL_HYBRID_TIMESTAMP
                : ts;
    }
}
