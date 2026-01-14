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

import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutorType;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.SharedComputeUtils;
import org.apache.ignite.marshalling.Marshaller;
import org.jetbrains.annotations.Nullable;

/** Packs job arguments and results. */
public final class ClientComputeJobPacker {
    /**
     * Packs compute job argument. If the marshaller is provided, it will be used to marshal the argument. If the marshaller is not provided
     * and the argument is a native column type or a tuple, it will be packed accordingly.
     *
     * @param arg Argument.
     * @param marshaller Marshaller.
     * @param packer Packer.
     * @param <T> Argument type.
     */
    public static <T> void packJobArgument(
            @Nullable T arg,
            @Nullable Marshaller<T, byte[]> marshaller,
            ClientMessagePacker packer,
            @Nullable Long observableTs) {
        pack(arg, marshaller, packer, observableTs);
    }

    /**
     * Packs compute job result. If the marshaller is provided, it will be used to marshal the result. If the marshaller is not provided and
     * the result is a native column type or a tuple, it will be packed accordingly.
     *
     * @param res Result.
     * @param marshaller Marshaller.
     * @param packer Packer.
     * @param <T> Result type.
     */
    public static <T> void packJobResult(@Nullable T res, @Nullable Marshaller<T, byte[]> marshaller, ClientMessagePacker packer) {
        pack(res, marshaller, packer, null);
    }

    /**
     * Packs job descriptor and argument.
     *
     * @param <T> Job argument type.
     * @param <R> Job result type.
     * @param descriptor Job descriptor.
     * @param arg Job argument.
     * @param platformComputeSupported Whether platform compute is supported.
     * @param w Packer.
     */
    public static <T, R> void packJob(
            JobDescriptor<T, R> descriptor,
            T arg,
            boolean platformComputeSupported,
            ClientMessagePacker w,
            @Nullable Long observableTs) {
        w.packDeploymentUnits(descriptor.units());

        w.packString(descriptor.jobClassName());
        w.packInt(descriptor.options().priority());
        w.packInt(descriptor.options().maxRetries());

        JobExecutorType executorType = descriptor.options().executorType();

        if (platformComputeSupported) {
            w.packInt(executorType.ordinal());
        } else if (executorType != JobExecutorType.JAVA_EMBEDDED) {
            throw new IllegalArgumentException("Custom job executors are not supported by the server: " + executorType);
        }

        packJobArgument(arg, descriptor.argumentMarshaller(), w, observableTs);
    }

    /** Packs object in the format: | typeId | value |. */
    private static <T> void pack(
            @Nullable T obj,
            @Nullable Marshaller<T, byte[]> marshaller,
            ClientMessagePacker packer,
            @Nullable Long observableTs) {
        ComputeJobDataHolder holder = obj instanceof ComputeJobDataHolder
                ? (ComputeJobDataHolder) obj
                : SharedComputeUtils.marshalArgOrResult(obj, marshaller);

        if (observableTs != null) {
            packer.packLong(observableTs);
        }

        if (holder.data() == null) {
            packer.packNil();
            return;
        }

        packer.packInt(holder.type().id());
        packer.packBinary(holder.data());
    }
}
