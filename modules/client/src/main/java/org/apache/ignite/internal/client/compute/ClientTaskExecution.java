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
import static org.apache.ignite.internal.client.compute.ClientJobExecution.getJobState;
import static org.apache.ignite.internal.client.compute.ClientJobExecution.getTaskState;
import static org.apache.ignite.internal.client.compute.ClientJobExecution.unpackJobState;
import static org.apache.ignite.internal.client.compute.ClientJobExecution.unpackTaskState;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.TaskState;
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

    // Local state cache
    private final CompletableFuture<@Nullable TaskState> stateFuture = new CompletableFuture<>();

    // Local states cache
    private final CompletableFuture<List<@Nullable JobState>> statesFutures = new CompletableFuture<>();

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
                        stateFuture.complete(unpackTaskState(payloadInputChannel));
                        statesFutures.complete(unpackJobStates(payloadInputChannel));
                        return result;
                    }
                });
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return resultAsync;
    }

    @Override
    public CompletableFuture<@Nullable TaskState> stateAsync() {
        if (stateFuture.isDone()) {
            return stateFuture;
        }
        return jobIdFuture.thenCompose(jobId -> getTaskState(ch, jobId));
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

    @Override
    public CompletableFuture<List<@Nullable JobState>> statesAsync() {
        if (statesFutures.isDone()) {
            return statesFutures;
        }

        return jobIdsFuture.thenCompose(ids -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<@Nullable JobState>[] futures = ids.stream()
                    .map(jobId -> getJobState(ch, jobId))
                    .toArray(CompletableFuture[]::new);

            return allOfToList(futures)
                    .thenApply(Function.identity());
        });
    }

    private static List<@Nullable JobState> unpackJobStates(PayloadInputChannel payloadInputChannel) {
        var unpacker = payloadInputChannel.in();
        var size = unpacker.unpackInt();

        if (size == 0) {
            return Collections.emptyList();
        }

        var states = new ArrayList<@Nullable JobState>(size);
        for (int i = 0; i < size; i++) {
            states.add(unpackJobState(payloadInputChannel));
        }

        return Collections.unmodifiableList(states);
    }
}
