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

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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

    private final CompletableFuture<JobIdCoordinatorId> jobIdCoordinatorId;

    private final CompletableFuture<R> resultAsync;

    public ClientJobExecution(ReliableChannel ch, CompletableFuture<PayloadInputChannel> reqFuture) {
        this.ch = ch;
        jobIdCoordinatorId = reqFuture
                .thenApply(r -> new JobIdCoordinatorId(r.in().unpackUuid(), r.in().unpackString()));

        resultAsync = reqFuture
                .thenCompose(PayloadInputChannel::notificationFuture)
                .thenApply(r -> {
                    // Notifications require explicit input close.
                    try (r) {
                        return (R) r.in().unpackObjectFromBinaryTuple();
                    }
                });
    }

    private static class JobIdCoordinatorId {
        JobIdCoordinatorId(UUID jobId, String coordinatorId) {
            this.jobId = jobId;
            this.coordinatorId = coordinatorId;
        }

        UUID jobId;
        String coordinatorId;
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return resultAsync;
    }

    @Override
    public CompletableFuture<JobStatus> statusAsync() {
        return jobIdCoordinatorId.thenCompose(this::getJobStatus);
    }

    @Override
    public CompletableFuture<Void> cancelAsync() {
        return jobIdCoordinatorId.thenCompose(this::cancelJob);
    }

    private CompletableFuture<JobStatus> getJobStatus(JobIdCoordinatorId jobIdCoordinatorId) {
        return ch.serviceAsync(
                ClientOp.COMPUTE_GET_STATUS,
                w -> w.out().packUuid(jobIdCoordinatorId.jobId),
                ClientJobExecution::unpackJobStatus,
                jobIdCoordinatorId.coordinatorId,
                null,
                false
        );
    }

    private CompletableFuture<Void> cancelJob(JobIdCoordinatorId jobIdCoordinatorId) {
        return ch.serviceAsync(
                ClientOp.COMPUTE_CANCEL,
                w -> w.out().packUuid(jobIdCoordinatorId.jobId),
                null,
                jobIdCoordinatorId.coordinatorId,
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
