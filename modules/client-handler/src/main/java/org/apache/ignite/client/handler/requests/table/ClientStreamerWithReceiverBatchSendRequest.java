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
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ColumnTypeConverter;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.ColumnType;
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
                    payloadElementCount,
                    payload);

            return jobExecution.resultAsync().thenApply(res -> {
                // TODO:
                // out.packCollectionAsBinaryTuple(returnResults ? res : null);
                out.packNil();

                return null;
            });
        });
    }

    private static class ReceiverRunnerJob implements ComputeJob<List<Object>> {
        @Override
        public List<Object> execute(JobExecutionContext context, Object... args) {
            int payloadElementCount = (int) args[0];
            byte[] payload = (byte[]) args[1];
            BinaryTupleReader reader = new BinaryTupleReader(payloadElementCount, payload);

            int readerIndex = 0;
            String receiverClassName = reader.stringValue(readerIndex++);

            if (receiverClassName == null) {
                throw new IgniteException(PROTOCOL_ERR, "Receiver class name is null");
            }

            int receiverArgsCount = reader.intValue(readerIndex++);

            List<Object> receiverArgs = new ArrayList<>(receiverArgsCount);
            for (int i = 0; i < receiverArgsCount; i++) {
                receiverArgs.add(ClientBinaryTupleUtils.readObject(reader, readerIndex));
                readerIndex += 3;
            }

            int typeId = reader.intValue(readerIndex++);
            ColumnType type = ColumnTypeConverter.fromIdOrThrow(typeId);
            Function<Integer, Object> itemReader = ClientBinaryTupleUtils.readerForType(reader, type);
            int itemsCount = reader.intValue(readerIndex++);

            List<Object> items = new ArrayList<>(itemsCount);
            for (int i = 0; i < itemsCount; i++) {
                items.add(itemReader.apply(readerIndex++));
            }

            ClassLoader classLoader = this.getClass().getClassLoader();
            Class<DataStreamerReceiver<Object, Object>> receiverClass = ComputeUtils.receiverClass(classLoader, receiverClassName);
            DataStreamerReceiver<Object, Object> receiver = ComputeUtils.instantiateReceiver(receiverClass);
            DataStreamerReceiverContext receiverContext = context::ignite;

            CompletableFuture<List<Object>> receiveFut = receiver.receive(items, receiverContext, receiverArgs);

            return receiveFut.join();
        }
    }
}
