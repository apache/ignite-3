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

package org.apache.ignite.client.handler.requests.compute;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.jetbrains.annotations.Nullable;

/**
 * Compute get state request.
 */
public class ClientComputeGetStateRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param compute Compute.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteComputeInternal compute
    ) {
        UUID jobId = in.unpackUuid();
        return compute.stateAsync(jobId).thenAccept(state -> packJobState(out, state));
    }

    /**
     * Writes a {@link JobState}.
     *
     * @param out Packer.
     * @param state Job state.
     */
    static void packJobState(ClientMessagePacker out, @Nullable JobState state) {
        if (state == null) {
            out.packNil();
        } else {
            out.packUuid(state.id());
            out.packInt(state.status().ordinal());
            out.packInstant(state.createTime());
            out.packInstant(state.startTime());
            out.packInstant(state.finishTime());
        }
    }
}
