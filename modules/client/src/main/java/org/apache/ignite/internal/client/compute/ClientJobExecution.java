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

package org.apache.ignite.internal.client.compute;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.lang.ErrorGroups.Compute.CANCELLING_ERR;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.jetbrains.annotations.Nullable;


/**
 * Client job execution implementation.
 */
class ClientJobExecution<R> implements JobExecution<R> {
    private final ReliableChannel ch;

    private final CompletableFuture<UUID> jobIdFuture;

    private final CompletableFuture<R> resultAsync;

    private final CompletableFuture<JobStatus> statusFuture = new CompletableFuture<>();

    public ClientJobExecution(ReliableChannel ch, CompletableFuture<PayloadInputChannel> reqFuture) {
        this.ch = ch;

        jobIdFuture = reqFuture.thenApply(r -> r.in().unpackUuid());

        resultAsync = reqFuture
                .thenCompose(PayloadInputChannel::notificationFuture)
                .thenApply(r -> {
                    // Notifications require explicit input close.
                    try (r) {
                        R result = (R) r.in().unpackObjectFromBinaryTuple();
                        statusFuture.complete(unpackJobStatus(r));
                        return result;
                    }
                });
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return resultAsync;
    }

    @Override
    public CompletableFuture<JobStatus> statusAsync() {
        if (statusFuture.isDone()) {
            return statusFuture;
        }
        return jobIdFuture.thenCompose(this::getJobStatus);
    }

    @Override
    public CompletableFuture<Void> cancelAsync() {
        if (statusFuture.isDone()) {
            return failedFuture(new ComputeException(CANCELLING_ERR, "Cancelling job " + statusFuture.join().id() + " failed."));
        }
        return jobIdFuture.thenCompose(this::cancelJob);
    }

    private CompletableFuture<JobStatus> getJobStatus(UUID jobId) {
        // Send the request to any node, the request will be broadcast since client doesn't know which particular node is running the job
        // especially in case of colocated execution.
        return ch.serviceAsync(
                ClientOp.COMPUTE_GET_STATUS,
                w -> w.out().packUuid(jobId),
                ClientJobExecution::unpackJobStatus,
                null,
                null,
                false
        );
    }

    private CompletableFuture<Void> cancelJob(UUID jobId) {
        // Send the request to any node, the request will be broadcast since client doesn't know which particular node is running the job
        // especially in case of colocated execution.
        return ch.serviceAsync(
                ClientOp.COMPUTE_CANCEL,
                w -> w.out().packUuid(jobId),
                null,
                null,
                null,
                false
        );
    }

    private static @Nullable JobStatus unpackJobStatus(PayloadInputChannel payloadInputChannel) {
        ClientMessageUnpacker unpacker = payloadInputChannel.in();
        if (unpacker.tryUnpackNil()) {
            return null;
        }
        return JobStatus.builder()
                .id(unpacker.unpackUuid())
                .state(JobState.valueOf(unpacker.unpackString()))
                .createTime(unpackInstant(unpacker))
                .startTime(unpackInstantNullable(unpacker))
                .finishTime(unpackInstantNullable(unpacker))
                .build();
    }

    private static @Nullable Instant unpackInstantNullable(ClientMessageUnpacker unpacker) {
        if (unpacker.tryUnpackNil()) {
            return null;
        }
        return unpackInstant(unpacker);
    }

    private static Instant unpackInstant(ClientMessageUnpacker unpacker) {
        long seconds = unpacker.unpackLong();
        int nanos = unpacker.unpackInt();
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
