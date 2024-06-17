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

import static org.apache.ignite.internal.client.compute.ClientJobExecution.cancelJob;
import static org.apache.ignite.internal.client.compute.ClientJobExecution.changePriority;
import static org.apache.ignite.internal.client.compute.ClientJobExecution.getJobStatus;
import static org.apache.ignite.internal.client.compute.ClientJobExecution.unpackJobStatus;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.jetbrains.annotations.Nullable;

/**
 * Client compute task implementation.
 *
 * @param <R> Task result type.
 */
class ClientTaskExecution<R> implements TaskExecution<R> {
    private final ReliableChannel ch;

    private final CompletableFuture<UUID> jobIdFuture;

    private final CompletableFuture<List<@Nullable UUID>> jobIdsFuture;

    private final CompletableFuture<R> resultAsync;

    // Local status cache
    private final CompletableFuture<@Nullable JobStatus> statusFuture = new CompletableFuture<>();

    // Local statuses cache
    private final CompletableFuture<List<@Nullable JobStatus>> statusesFutures = new CompletableFuture<>();

    ClientTaskExecution(ReliableChannel ch, CompletableFuture<SubmitTaskResult> reqFuture) {
        this.ch = ch;

        jobIdFuture = reqFuture.thenApply(SubmitTaskResult::jobId);
        jobIdsFuture = reqFuture.thenApply(SubmitTaskResult::jobIds);

        resultAsync = reqFuture
                .thenCompose(SubmitTaskResult::notificationFuture)
                .thenApply(payloadInputChannel -> {
                    // Notifications require explicit input close.
                    try (payloadInputChannel) {
                        R result = (R) payloadInputChannel.in().unpackObjectFromBinaryTuple();
                        statusFuture.complete(unpackJobStatus(payloadInputChannel));
                        statusesFutures.complete(unpackJobStatuses(payloadInputChannel));
                        return result;
                    }
                });
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return resultAsync;
    }

    @Override
    public CompletableFuture<@Nullable JobStatus> statusAsync() {
        if (statusFuture.isDone()) {
            return statusFuture;
        }
        return jobIdFuture.thenCompose(jobId -> getJobStatus(ch, jobId));
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        if (statusFuture.isDone()) {
            return falseCompletedFuture();
        }
        return jobIdFuture.thenCompose(jobId -> cancelJob(ch, jobId));
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        if (statusFuture.isDone()) {
            return falseCompletedFuture();
        }
        return jobIdFuture.thenCompose(jobId -> changePriority(ch, jobId, newPriority));
    }

    @Override
    public CompletableFuture<List<@Nullable JobStatus>> statusesAsync() {
        if (statusesFutures.isDone()) {
            return statusesFutures;
        }

        return jobIdsFuture.thenCompose(ids -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<@Nullable JobStatus>[] futures = ids.stream()
                    .map(jobId -> getJobStatus(ch, jobId))
                    .toArray(CompletableFuture[]::new);

            return allOfToList(futures)
                    .thenApply(Function.identity());
        });
    }

    private static List<@Nullable JobStatus> unpackJobStatuses(PayloadInputChannel payloadInputChannel) {
        var unpacker = payloadInputChannel.in();
        var size = unpacker.unpackInt();

        if (size == 0) {
            return Collections.emptyList();
        }

        var statuses = new ArrayList<@Nullable JobStatus>(size);
        for (int i = 0; i < size; i++) {
            var status = unpackJobStatus(payloadInputChannel);
            statuses.add(status);
        }

        return Collections.unmodifiableList(statuses);
    }
}
