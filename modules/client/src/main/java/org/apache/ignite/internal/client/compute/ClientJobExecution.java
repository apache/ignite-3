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

import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.Marshaller;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.compute.JobStateImpl;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;


/**
 * Client job execution implementation.
 */
class ClientJobExecution<R> implements JobExecution<R> {
    private static final JobStatus[] JOB_STATUSES = JobStatus.values();

    private final ReliableChannel ch;

    private final CompletableFuture<UUID> jobIdFuture;

    private final CompletableFuture<R> resultAsync;

    private final Marshaller<R, byte[]> marshaler;

    // Local state cache
    private final CompletableFuture<@Nullable JobState> stateFuture = new CompletableFuture<>();

    ClientJobExecution(ReliableChannel ch, CompletableFuture<SubmitResult> reqFuture, Marshaller<R, byte[]> marshaler) {
        this.ch = ch;

        this.jobIdFuture = reqFuture.thenApply(SubmitResult::jobId);

        this.marshaler = marshaler;

        resultAsync = reqFuture
                .thenCompose(SubmitResult::notificationFuture)
                .thenApply(r -> {
                    // Notifications require explicit input close.
                    try (r) {
                        R result = marshaler.unmarshal((byte[]) r.in().unpackObjectFromBinaryTuple());
                        stateFuture.complete(unpackJobState(r));
                        return result;
                    }
                });
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return resultAsync;
    }

    @Override
    public CompletableFuture<@Nullable JobState> stateAsync() {
        if (stateFuture.isDone()) {
            return stateFuture;
        }
        return jobIdFuture.thenCompose(jobId -> getJobState(ch, jobId));
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        if (stateFuture.isDone()) {
            return falseCompletedFuture();
        }
        return jobIdFuture.thenCompose(jobId -> cancelJob(ch, jobId));
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        if (stateFuture.isDone()) {
            return falseCompletedFuture();
        }
        return jobIdFuture.thenCompose(jobId -> changePriority(ch, jobId, newPriority));
    }

    static CompletableFuture<@Nullable JobState> getJobState(ReliableChannel ch, UUID jobId) {
        // Send the request to any node, the request will be broadcast since client doesn't know which particular node is running the job
        // especially in case of colocated execution.
        return ch.serviceAsync(
                ClientOp.COMPUTE_GET_STATE,
                w -> w.out().packUuid(jobId),
                ClientJobExecution::unpackJobState,
                null,
                null,
                false
        );
    }

    static CompletableFuture<@Nullable Boolean> cancelJob(ReliableChannel ch, UUID jobId) {
        // Send the request to any node, the request will be broadcast since client doesn't know which particular node is running the job
        // especially in case of colocated execution.
        return ch.serviceAsync(
                ClientOp.COMPUTE_CANCEL,
                w -> w.out().packUuid(jobId),
                ClientJobExecution::unpackBooleanResult,
                null,
                null,
                false
        );
    }

    static CompletableFuture<@Nullable Boolean> changePriority(ReliableChannel ch, UUID jobId, int newPriority) {
        // Send the request to any node, the request will be broadcast since client doesn't know which particular node is running the job
        // especially in case of colocated execution.
        return ch.serviceAsync(
                ClientOp.COMPUTE_CHANGE_PRIORITY,
                w -> {
                    w.out().packUuid(jobId);
                    w.out().packInt(newPriority);
                },
                ClientJobExecution::unpackBooleanResult,
                null,
                null,
                false
        );
    }

    static @Nullable JobState unpackJobState(PayloadInputChannel payloadInputChannel) {
        ClientMessageUnpacker unpacker = payloadInputChannel.in();
        if (unpacker.tryUnpackNil()) {
            return null;
        }
        return JobStateImpl.builder()
                .id(unpacker.unpackUuid())
                .status(unpackJobStatus(unpacker))
                .createTime(unpacker.unpackInstant())
                .startTime(unpacker.unpackInstantNullable())
                .finishTime(unpacker.unpackInstantNullable())
                .build();
    }

    private static @Nullable Boolean unpackBooleanResult(PayloadInputChannel payloadInputChannel) {
        ClientMessageUnpacker unpacker = payloadInputChannel.in();
        if (unpacker.tryUnpackNil()) {
            return null;
        }
        return unpacker.unpackBoolean();
    }

    private static JobStatus unpackJobStatus(ClientMessageUnpacker unpacker) {
        int id = unpacker.unpackInt();
        if (id >= 0 && id < JOB_STATUSES.length) {
            return JOB_STATUSES[id];
        }
        throw new IgniteException(PROTOCOL_ERR, "Invalid job status id: " + id);
    }
}
