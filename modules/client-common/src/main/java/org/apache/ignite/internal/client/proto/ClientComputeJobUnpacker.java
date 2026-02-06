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

package org.apache.ignite.internal.client.proto;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobExecutorType;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.ComputeJobDataType;
import org.apache.ignite.internal.compute.SharedComputeUtils;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.UnmarshallingException;
import org.jetbrains.annotations.Nullable;

/** Unpacks job results. */
public final class ClientComputeJobUnpacker {
    /**
     * Unpacks compute job result. If the marshaller is provided, it will be used to unmarshal the result. If the marshaller is not provided
     * and the result class is provided and the result is a tuple, it will be unpacked as a pojo of that class. If the marshaller is not
     * provided and the result is a native column type or a tuple, it will be unpacked accordingly.
     *
     * @param unpacker Unpacker.
     * @param marshaller Marshaller.
     * @param resultClass Result class.
     * @return Unpacked result.
     */
    public static @Nullable Object unpackJobResult(
            ClientMessageUnpacker unpacker,
            @Nullable Marshaller<?, byte[]> marshaller,
            @Nullable Class<?> resultClass
    ) {
        ComputeJobDataHolder holder = unpackJobArgumentWithoutMarshaller(unpacker, false);

        return SharedComputeUtils.unmarshalArgOrResult(holder, marshaller, resultClass);
    }

    /** Unpacks compute job argument without marshaller. */
    public static @Nullable ComputeJobDataHolder unpackJobArgumentWithoutMarshaller(
            ClientMessageUnpacker unpacker,
            boolean enableObservableTs) {
        long observableTs = enableObservableTs
                ? unpacker.unpackLong()
                : HybridTimestamp.NULL_HYBRID_TIMESTAMP;

        if (unpacker.tryUnpackNil()) {
            return observableTs == HybridTimestamp.NULL_HYBRID_TIMESTAMP
                    ? null
                    : new ComputeJobDataHolder(ComputeJobDataType.NATIVE, null, observableTs);
        }

        int typeId = unpacker.unpackInt();
        ComputeJobDataType type = ComputeJobDataType.fromId(typeId);
        if (type == null) {
            throw new UnmarshallingException("Unsupported compute job type id: " + typeId);
        }

        return new ComputeJobDataHolder(type, unpacker.readBinary(), observableTs);
    }

    /** Unpacks compute job info. */
    public static Job unpackJob(
            ClientMessageUnpacker unpacker,
            boolean enablePlatformJobs,
            boolean enableObservableTs) {
        List<DeploymentUnit> deploymentUnits = unpacker.unpackDeploymentUnits();
        String jobClassName = unpacker.unpackString();
        var options = JobExecutionOptions.builder().priority(unpacker.unpackInt()).maxRetries(unpacker.unpackInt());

        if (enablePlatformJobs) {
            options.executorType(JobExecutorType.fromOrdinal(unpacker.unpackInt()));
        }

        ComputeJobDataHolder args = unpackJobArgumentWithoutMarshaller(unpacker, enableObservableTs);

        return new Job(deploymentUnits, jobClassName, options.build(), args);
    }

    /**
     * Unpacks compute job task ID.
     *
     * @param unpacker Unpacker.
     * @param enableTaskId {@code true} if {@link ProtocolBitmaskFeature#COMPUTE_TASK_ID} is supported.
     * @return Task ID.
     */
    public static @Nullable UUID unpackTaskId(ClientMessageUnpacker unpacker, boolean enableTaskId) {
        return enableTaskId ? unpacker.unpackUuidNullable() : null;
    }

    /** Job info. */
    public static class Job {
        private final List<DeploymentUnit> deploymentUnits;
        private final String jobClassName;
        private final JobExecutionOptions options;
        private final @Nullable ComputeJobDataHolder args;

        private Job(
                List<DeploymentUnit> deploymentUnits,
                String jobClassName,
                JobExecutionOptions options,
                @Nullable ComputeJobDataHolder args) {
            this.deploymentUnits = deploymentUnits;
            this.jobClassName = jobClassName;
            this.options = options;
            this.args = args;
        }

        public List<DeploymentUnit> deploymentUnits() {
            return deploymentUnits;
        }

        public String jobClassName() {
            return jobClassName;
        }

        public JobExecutionOptions options() {
            return options;
        }

        public @Nullable ComputeJobDataHolder arg() {
            return args;
        }
    }
}
