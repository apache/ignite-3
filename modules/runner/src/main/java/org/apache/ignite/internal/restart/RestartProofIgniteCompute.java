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

package org.apache.ignite.internal.restart;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.lang.CancellationToken;
import org.jetbrains.annotations.Nullable;

/**
 * Reference to {@link IgniteCompute} under a swappable {@link Ignite} instance. When a restart happens, this switches to
 * the new Ignite instance.
 *
 * <p>API operations on this are linearized with respect to node restarts. Normally (except for situations when timeouts trigger), user
 * operations will not interact with detached objects.
 */
// TODO; IGNITE-23166 - make returned executions restart-proof.
class RestartProofIgniteCompute implements IgniteCompute, Wrapper {
    private final IgniteAttachmentLock attachmentLock;

    RestartProofIgniteCompute(IgniteAttachmentLock attachmentLock) {
        this.attachmentLock = attachmentLock;
    }

    @Override
    public <T, R> CompletableFuture<JobExecution<R>> submitAsync(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return attachmentLock.attachedAsync(ignite -> ignite.compute().submitAsync(target, descriptor, arg, cancellationToken));
    }

    @Override
    public <T, R> CompletableFuture<BroadcastExecution<R>> submitAsync(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return attachmentLock.attachedAsync(ignite -> ignite.compute().submitAsync(target, descriptor, arg, cancellationToken));
    }

    @Override
    public <T, R> R execute(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return attachmentLock.attached(ignite -> ignite.compute().execute(target, descriptor, arg, cancellationToken));
    }

    @Override
    public <T, R> Collection<R> execute(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return attachmentLock.attached(ignite -> ignite.compute().execute(target, descriptor, arg, cancellationToken));
    }

    @Override
    public <T, R> TaskExecution<R> submitMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return attachmentLock.attached(ignite -> ignite.compute().submitMapReduce(taskDescriptor, arg, cancellationToken));
    }

    @Override
    public <T, R> CompletableFuture<R> executeMapReduceAsync(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return attachmentLock.attachedAsync(ignite -> ignite.compute().executeMapReduceAsync(taskDescriptor, arg, cancellationToken));
    }

    @Override
    public <T, R> R executeMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return attachmentLock.attached(ignite -> ignite.compute().executeMapReduce(taskDescriptor, arg, cancellationToken));
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return attachmentLock.attached(ignite -> Wrappers.unwrap(ignite.compute(), classToUnwrap));
    }
}
