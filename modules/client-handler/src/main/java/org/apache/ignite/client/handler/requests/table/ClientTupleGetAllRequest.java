/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.table.manager.IgniteTables;

import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTable;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTuples;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.writeTuples;

/**
 * Client tuple get all request.
 */
public class ClientTupleGetAllRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param tables Ignite tables.
     * @return Future.
     * @throws IOException On serialization error.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteTables tables
    ) throws IOException {
        var table = readTable(in, tables);
        var keyTuples = readTuples(in, table, true);

        return table.getAllAsync(keyTuples).thenAccept(tuples -> writeTuples(out, tuples));
    }
}
