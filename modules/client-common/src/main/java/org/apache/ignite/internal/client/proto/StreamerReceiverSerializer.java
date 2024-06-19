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

package org.apache.ignite.internal.client.proto;

import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Streamer receiver serializer.
 */
public class StreamerReceiverSerializer {
    /**
     * Serializes streamer receiver info.
     *
     * @param w Writer.
     * @param receiverClassName Receiver class name.
     * @param receiverArgs Receiver arguments.
     * @param items Items.
     */
    public static void serialize(ClientMessagePacker w, String receiverClassName, Object[] receiverArgs, Collection<?> items) {
        // className + args size + args + items size + item type + items.
        int binaryTupleSize = 1 + 1 + receiverArgs.length * 3 + 1 + 1 + items.size();
        var builder = new BinaryTupleBuilder(binaryTupleSize);
        builder.appendString(receiverClassName);
        builder.appendInt(receiverArgs.length);

        for (var arg : receiverArgs) {
            ClientBinaryTupleUtils.appendObject(builder, arg);
        }

        ClientBinaryTupleUtils.appendCollectionToBinaryTuple(builder, items);

        w.packObjectAsBinaryTuple(builder.build().array());
    }

    /**
     * Deserializes streamer receiver info.
     *
     * @param bytes Bytes.
     * @return Streamer receiver info.
     */
    public static SteamerReceiverInfo deserialize(byte[] bytes) {
        var reader = new BinaryTupleReader(bytes.length, bytes);

        int readerIndex = 0;
        String receiverClassName = reader.stringValue(readerIndex++);

        if (receiverClassName == null) {
            throw new IgniteException(PROTOCOL_ERR, "Receiver class name is null");
        }

        int receiverArgsCount = reader.intValue(readerIndex++);

        Object[] receiverArgs = new Object[receiverArgsCount];
        for (int i = 0; i < receiverArgsCount; i++) {
            receiverArgs[i] = ClientBinaryTupleUtils.readObject(reader, readerIndex);
            readerIndex += 3;
        }

        List<Object> items = ClientBinaryTupleUtils.readCollectionFromBinaryTuple(reader, readerIndex);

        return new SteamerReceiverInfo(receiverClassName, receiverArgs, items);
    }

    /**
     * Serializes receiver results.
     *
     * @param w Writer.
     * @param receiverResults Receiver results.
     */
    public static void serializeResults(ClientMessagePacker w, @Nullable List<Object> receiverResults) {
        if (receiverResults == null || receiverResults.isEmpty()) {
            w.packNil();
            return;
        }

        int numElements = 2 + receiverResults.size();
        var builder = new BinaryTupleBuilder(numElements);
        ClientBinaryTupleUtils.appendCollectionToBinaryTuple(builder, receiverResults);

        w.packInt(numElements);
        w.packBinaryTuple(builder);
    }

    /**
     * Deserializes receiver results.
     *
     * @param r Reader.
     * @return Receiver results.
     */
    public static @Nullable <R> List<R> deserializeResults(ClientMessageUnpacker r) {
        if (r.tryUnpackNil()) {
            return null;
        }

        int numElements = r.unpackInt();
        byte[] bytes = r.readBinary();
        var reader = new BinaryTupleReader(numElements, bytes);

        return ClientBinaryTupleUtils.readCollectionFromBinaryTuple(reader, 0);
    }

    /**
     * Streamer receiver info.
     */
    public static class SteamerReceiverInfo {
        private final String className;
        private final Object[] args;
        private final List<Object> items;

        private SteamerReceiverInfo(String className, Object[] args, List<Object> items) {
            this.className = className;
            this.args = args;
            this.items = items;
        }

        /**
         * Gets receiver class name.
         *
         * @return Receiver class name.
         */
        public String className() {
            return className;
        }

        /**
         * Get receiver args.
         *
         * @return Receiver args.
         */
        public Object[] args() {
            return args;
        }

        /**
         * Gets items.
         *
         * @return Items.
         */
        public List<Object> items() {
            return items;
        }
    }
}
