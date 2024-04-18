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
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.jetbrains.annotations.Nullable;

/**
 * Compute execute request.
 */
public class ClientComputeGetStatusRequest {
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
        return compute.statusAsync(jobId).thenAccept(jobStatus -> packJobStatus(out, jobStatus));
    }

    /**
     * Writes a {@link JobStatus}.
     *
     * @param out Packer.
     * @param jobStatus Job status.
     */
    static void packJobStatus(ClientMessagePacker out, @Nullable JobStatus jobStatus) {
        if (jobStatus == null) {
            out.packNil();
        } else {
            out.packUuid(jobStatus.id());
            out.packInt(jobStatus.state().ordinal());
            out.packInstant(jobStatus.createTime());
            out.packInstant(jobStatus.startTime());
            out.packInstant(jobStatus.finishTime());
        }
    }
}
