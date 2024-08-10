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

import static org.apache.ignite.internal.compute.ComputeUtils.convertToComputeFuture;
import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.convertToPublicFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.marshalling.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps the future of {@link JobExecution} converting exceptions thrown by the delegate to public.
 *
 * @param <R> Result type.
 */
class JobExecutionFutureWrapper<R> implements JobExecution<R>, MarshallerProvider<R> {
    private final CompletableFuture<JobExecution<R>> delegate;

    JobExecutionFutureWrapper(CompletableFuture<JobExecution<R>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return convertToComputeFuture(delegate.thenCompose(JobExecution::resultAsync));
    }

    @Override
    public CompletableFuture<@Nullable JobState> stateAsync() {
        return convertToPublicFuture(delegate.thenCompose(JobExecution::stateAsync));
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        return convertToPublicFuture(delegate.thenCompose(JobExecution::cancelAsync));
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        return convertToPublicFuture(delegate.thenCompose(jobExecution -> jobExecution.changePriorityAsync(newPriority)));
    }

    @Override
    public Marshaller<R, byte[]> resultMarshaller() {
        try {
            return delegate.thenApply(d -> {
                if (d instanceof MarshallerProvider) {
                    return ((MarshallerProvider<R>) d).resultMarshaller();
                }

                throw new IllegalArgumentException(
                        "Can not return marshaller because " + d.getClass().getName()
                                + " does not implement " + MarshallerProvider.class.getName()
                );
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
