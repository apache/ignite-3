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

package org.apache.ignite.compute;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.marshaling.Marshaler;
import org.jetbrains.annotations.Nullable;

/**
 * Core Ignite Compute Job interface. If you want to define your own job, you should implement this interface and
 * deploy the job to the cluster with Deployment API. Then, you can execute this job on the cluster by calling
 * {@link IgniteCompute} APIs.
 *
 * <p>If you want to pass/return custom data structures to/from the job, you should also implement {@link Marshaler}
 * and return it from {@link #inputMarshaler()} and {@link #resultMarshaler()} methods.
 *
 * @param <T> Type of the job argument.
 * @param <R> Type of the job result.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface ComputeJob<T, R> {
    /**
     * Executes the job on an Ignite node.
     *
     * @param context The execution context.
     * @param arg Job arguments.
     * @return Job future. Can be null if the job is synchronous and does not return any result.
     */
    @Nullable CompletableFuture<R> executeAsync(JobExecutionContext context, @Nullable T arg);

    /**
     * Marshaler for the input argument. Default is {@code null} meaning that only primitive types are supported.
     *
     * @return Input marshaler.
     */
    default @Nullable Marshaler<T, byte[]> inputMarshaler() {
        return null;
    }

    /**
     * Marshaler for the job result. Default is {@code null} meaning that only primitive types are supported.
     *
     * @return Result marshaler.
     */
    default @Nullable Marshaler<R, byte[]> resultMarshaler() {
        return null;
    }
}
