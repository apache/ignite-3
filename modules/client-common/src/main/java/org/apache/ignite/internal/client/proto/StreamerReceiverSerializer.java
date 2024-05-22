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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnType;

/**
 * Streamer receiver serializer.
 */
public class StreamerReceiverSerializer {
    public static void serialize(ClientMessagePacker w, String receiverClassName, Object[] receiverArgs, Collection<?> items) {
        // className + args size + args + items size + item type + items.
        int binaryTupleSize = 1 + 1 + receiverArgs.length * 3 + 1 + 1 + items.size();
        var builder = new BinaryTupleBuilder(binaryTupleSize);
        builder.appendString(receiverClassName);
        builder.appendInt(receiverArgs.length);

        for (var arg : receiverArgs) {
            ClientBinaryTupleUtils.appendObject(builder, arg);
        }

        ClientBinaryTupleUtils.writeCollectionToBinaryTuple(builder, items);

        w.packInt(binaryTupleSize);
        w.packBinaryTuple(builder);
    }

    public static SteamerReceiverInfo deserialize(byte[] bytes, int elementCount) {
        var reader = new BinaryTupleReader(elementCount, bytes);

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

        int typeId = reader.intValue(readerIndex++);
        ColumnType type = ColumnTypeConverter.fromIdOrThrow(typeId);
        Function<Integer, Object> itemReader = ClientBinaryTupleUtils.readerForType(reader, type);
        int itemsCount = reader.intValue(readerIndex++);

        List<Object> items = new ArrayList<>(itemsCount);
        for (int i = 0; i < itemsCount; i++) {
            items.add(itemReader.apply(readerIndex++));
        }

        return new SteamerReceiverInfo(receiverClassName, receiverArgs, items);
    }

    public static class SteamerReceiverInfo {
        private final String className;
        private final Object[] args;
        private final List<Object> items;

        private SteamerReceiverInfo(String className, Object[] args, List<Object> items) {
            this.className = className;
            this.args = args;
            this.items = items;
        }

        public String className() {
            return className;
        }

        public Object[] args() {
            return args;
        }

        public List<Object> items() {
            return items;
        }
    }
}
