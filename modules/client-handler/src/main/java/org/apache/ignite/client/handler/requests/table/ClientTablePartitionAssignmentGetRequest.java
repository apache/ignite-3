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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;

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
    public static CompletableFuture<Object> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteTablesInternal tables
    ) throws NodeStoppingException {
        int tableId = in.unpackInt();
        var assignment = tables.assignments(tableId);

        if (assignment == null) {
            out.packArrayHeader(0);
            return null;
        }

        out.packArrayHeader(assignment.size());

        for (String leaderNodeId : assignment) {
            out.packString(leaderNodeId);
        }

        return null;
    }
}
