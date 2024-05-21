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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
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
            IgniteComputeInternal compute
    ) {
        return readTableAsync(in, tables).thenCompose(table -> {
                    int partition = in.unpackInt();
                    List<DeploymentUnit> deploymentUnits = in.unpackDeploymentUnits();
                    boolean returnResults = in.unpackBoolean();

                    // receiverClassName, receiverArgs, items
                    int payloadElementCount = in.unpackInt();
                    byte[] payload = in.readBinary();
                    Set<ClusterNode> candidateNodes = Set.of(); // TODO

                    var jobExecution = compute.executeAsyncWithFailover(
                            candidateNodes,
                            deploymentUnits,
                            ReceiverRunnerJob.class.getName(),
                            JobExecutionOptions.DEFAULT,
                            payloadFieldCount,
                            payload);

                    return jobExecution.resultAsync().thenApply(res -> {
                        // TODO:
                        // out.packCollectionAsBinaryTuple(returnResults ? res : null);
                        out.packNil();

                        return null;
                    });
                });

            // TODO: use Compute component to execute receiver on specific node with failover, proper executor, etc.
            // use this.getClass().classLoader() to get the class loader which Compute has prepared for us.
//            Class<DataStreamerReceiver<Object, Object>> receiverClass = ComputeUtils.receiverClass(
//                    ClassLoader.getSystemClassLoader(), receiverClassName);
//
//            DataStreamerReceiver<Object, Object> receiver = ComputeUtils.instantiateReceiver(receiverClass);
//            DataStreamerReceiverContext context = () -> ignite;
//
//            return receiver.receive(items, context, receiverArgs)
//                    .thenApply(res -> {
//                        out.packCollectionAsBinaryTuple(returnResults ? res : null);
//
//                        return null;
//                    });
    }

    private static class ReceiverRunnerJob implements ComputeJob<List<Object>> {
        @Override
        public List<Object> execute(JobExecutionContext context, Object... args) {
            int payloadElementCount = (int) args[0];
            byte[] payload = (byte[]) args[1];
            BinaryTupleReader reader = new BinaryTupleReader(payloadElementCount, payload);

            String receiverClassName = reader.stringValue(0);
            int receiverArgsCount = reader.intValue(1);

            List<Object> receiverArgs = new ArrayList<>(receiverArgsCount);
            for (int i = 0; i < receiverArgsCount; i++) {
                receiverArgs.add(ClientBinaryTupleUtils.readObject(reader, 2 + i * 3));
            }

            int itemsCount = reader.intValue(2 + receiverArgsCount * 3);


            DataStreamerReceiver<Object, Object> receiver = ComputeUtils.instantiateReceiver(receiverClass);
            DataStreamerReceiverContext receiverContext = context::ignite;
            return List.of();
        }
    }
}
