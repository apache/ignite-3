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

package org.apache.ignite.internal.compute.streamer;

import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.JobExecutionContextImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.internal.client.proto;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
import org.jetbrains.annotations.Nullable;

/**
 * Internal compute job that executes user-defined data streamer receiver.
 */
public class StreamerReceiverJob implements ComputeJob<byte[], byte[]> {
    @Override
    public @Nullable CompletableFuture<byte[]> executeAsync(JobExecutionContext context, byte @Nullable [] payload) {
        assert payload != null : "Streamer receiver job argument is null";

        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        int payloadElementCount = buf.getInt();

        var reader = new BinaryTupleReader(payloadElementCount, buf.slice().order(ByteOrder.LITTLE_ENDIAN));

        int readerIndex = 0;
        String receiverClassName = reader.stringValue(readerIndex++);

        if (receiverClassName == null) {
            throw new IgniteException(PROTOCOL_ERR, "Receiver class name is null");
        }

        Object receiverArg = ClientBinaryTupleUtils.readObject(reader, readerIndex);

        readerIndex += 3;

        List<Object> items = ClientBinaryTupleUtils.readCollectionFromBinaryTuple(reader, readerIndex);


        ClassLoader classLoader = ((JobExecutionContextImpl) context).classLoader();
        Class<DataStreamerReceiver<Object, Object, Object>> receiverClass = ComputeUtils.receiverClass(
                classLoader, receiverClassName
        );
        DataStreamerReceiver<Object, Object, Object> receiver = ComputeUtils.instantiateReceiver(receiverClass);
        DataStreamerReceiverContext receiverContext = context::ignite;

        CompletableFuture<List<Object>> receiverRes = receiver.receive(items, receiverContext, receiverArg);

        if (receiverRes == null) {
            return CompletableFuture.completedFuture(null);
        }

        // TODO: Serialize here, avoid re-serializing before passing to the client.
        return receiverRes.thenApply(r -> new byte[0]);
    }
}
