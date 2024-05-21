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

import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTableAsync;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
import org.apache.ignite.table.manager.IgniteTables;

/**
 * Client streamer batch request.
 */
public class ClientStreamerWithReceiverBatchSendRequest {
    /**
     * Processes the request.
     *
     * @param in        Unpacker.
     * @param out       Packer.
     * @param tables    Ignite tables.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteTables tables,
            Ignite ignite
    ) {
        return readTableAsync(in, tables).thenCompose(table -> {
            int partition = in.unpackInt();
            List<DeploymentUnit> deploymentUntis = in.unpackDeploymentUnits();
            String receiverClassName = in.unpackString();
            Object[] receiverArgs = in.unpackObjectArrayFromBinaryTuple();
            boolean returnResults = in.unpackBoolean();
            List<Object> items = in.unpackCollectionFromBinaryTuple();

            if (items == null) {
                throw new IgniteException(PROTOCOL_ERR, "Data streamer items are null.");
            }

            // TODO: Get class loader from units.
            // TODO: Execute on specified partition.
            // TODO: Use compute executor.
            Class<DataStreamerReceiver<Object, Object>> receiverClass = ComputeUtils.receiverClass(
                    ClassLoader.getSystemClassLoader(), receiverClassName);

            DataStreamerReceiver<Object, Object> receiver = ComputeUtils.instantiateReceiver(receiverClass);
            DataStreamerReceiverContext context = () -> ignite;

            return receiver.receive(items, context, receiverArgs)
                    .thenApply(res -> {
                        out.packCollectionAsBinaryTuple(returnResults ? res : null);

                        return null;
                    });
        });
    }
}
