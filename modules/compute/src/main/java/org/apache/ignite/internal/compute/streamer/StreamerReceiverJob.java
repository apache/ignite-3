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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.client.proto.StreamerReceiverSerializer;
import org.apache.ignite.internal.client.proto.StreamerReceiverSerializer.SteamerReceiverInfo;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.JobExecutionContextImpl;
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

        SteamerReceiverInfo receiverInfo = StreamerReceiverSerializer.deserializeReceiverInfo(
                buf.slice().order(ByteOrder.LITTLE_ENDIAN),
                payloadElementCount);

        ClassLoader classLoader = ((JobExecutionContextImpl) context).classLoader();
        Class<DataStreamerReceiver<Object, Object, Object>> receiverClass = ComputeUtils.receiverClass(
                classLoader, receiverInfo.className()
        );
        DataStreamerReceiver<Object, Object, Object> receiver = ComputeUtils.instantiateReceiver(receiverClass);
        DataStreamerReceiverContext receiverContext = context::ignite;

        CompletableFuture<List<Object>> receiverRes = receiver.receive(receiverInfo.items(), receiverContext, receiverInfo.arg());

        if (receiverRes == null) {
            return CompletableFuture.completedFuture(null);
        }

        return receiverRes.thenApply(StreamerReceiverSerializer::serializeReceiverJobResults);
    }
}
