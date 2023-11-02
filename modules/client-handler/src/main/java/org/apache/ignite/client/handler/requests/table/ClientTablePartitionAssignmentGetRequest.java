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

package org.apache.ignite.client.handler.requests.table;

import static org.apache.ignite.internal.tracing.TracingManager.asyncSpan;
import static org.apache.ignite.internal.tracing.TracingManager.span;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.lang.IgniteException;

/**
 * Client partition assignment retrieval request.
 */
public class ClientTablePartitionAssignmentGetRequest {
    /**
     * Processes the request.
     *
     * @param in     Unpacker.
     * @param out    Packer.
     * @param tables Ignite tables.
     * @return Future.
     * @throws IgniteException When schema registry is no initialized.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteTablesInternal tables
    ) throws NodeStoppingException {
        var span = asyncSpan("ClientTablePartitionAssignmentGetRequest.process");

        try (span) {
            int tableId = in.unpackInt();

            return span.wrap(tables.assignmentsAsync(tableId).thenAccept(assignment -> {
                if (assignment == null) {
                    out.packInt(0);
                    return;
                }

                out.packInt(assignment.size());

                for (String leaderNodeId : assignment) {
                    out.packString(leaderNodeId);
                }
            }));
        } catch (Exception e) {
            span.recordException(e);

            throw e;
        }
    }
}
