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

package org.apache.ignite.client.handler.requests.compute;

import static org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteRequest.unpackArgs;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTable;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTuple;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.table.manager.IgniteTables;

/**
 * Compute execute colocated request.
 */
public class ClientComputeExecuteColocatedRequest {
    /**
     * Processes the request.
     *
     * @param in        Unpacker.
     * @param out       Packer.
     * @param compute   Compute.
     * @param tables    Tables.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteCompute compute,
            IgniteTables tables) {
        var table = readTable(in, tables);
        var keyTuple = readTuple(in, table, true);

        String jobClassName = in.unpackString();
        Object[] args = unpackArgs(in);

        return compute.executeColocated(table.name(), keyTuple, jobClassName, args).thenAccept(out::packObjectWithType);
    }
}
