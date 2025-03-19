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

import static org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils.unsupportedTypeException;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.ReceiverDescriptor;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Streamer receiver serializer.
 *
 * <p>Client streamer: client -> handler -> job -> handler -> client:
 * Avoid deserializing receiver payload and results on handler side and pass byte array as is.
 *
 * <p>Embedded streamer: node -> job -> node:
 * No intermediate steps, trivial serialize/deserialize.
 */
public class StreamerReceiverSerializer {
    private static final int TYPE_ID_TUPLE = -64;

    /**
     * Serializes streamer receiver info.
     *
     * @param w Writer.
     * @param receiverClassName Receiver class name.
     * @param receiverArg Receiver arguments.
     * @param items Items.
     */
    public static <A> void serializeReceiverInfoOnClient(
            ClientMessagePacker w,
            String receiverClassName,
            A receiverArg,
            @Nullable Marshaller<A, byte[]> receiverArgsMarshaller,
            Collection<?> items) {
        // className + arg + items size + item type + items.
        int binaryTupleSize = 1 + 3 + 1 + 1 + items.size();
        var builder = new BinaryTupleBuilder(binaryTupleSize);
        builder.appendString(receiverClassName);

        // TODO: Use SharedComputeUtils for arg and items.
        ClientBinaryTupleUtils.appendObject(builder, receiverArg);

        appendCollectionToBinaryTuple(builder, items);

        w.packInt(binaryTupleSize);
        w.packBinaryTuple(builder);
    }

    /**
     * Serializes streamer receiver info.
     *
     * @param receiver Receiver descriptor.
     * @param receiverArg Receiver arguments.
     * @param items Items.
     */
    public static <A> byte[] serializeReceiverInfoWithElementCount(
            ReceiverDescriptor<A> receiver,
            @Nullable A receiverArg,
            Collection<?> items) {
        // className + arg + items size + item type + items.
        int binaryTupleSize = 1 + 3 + 1 + 1 + items.size();
        var builder = new BinaryTupleBuilder(binaryTupleSize);
        builder.appendString(receiver.receiverClassName());

        // TODO: Use SharedComputeUtils for arg and items.
        ClientBinaryTupleUtils.appendObject(builder, receiverArg);

        appendCollectionToBinaryTuple(builder, items);

        ByteBuffer buf = builder.build();
        int bufSize = buf.limit() - buf.position();
        byte[] res = new byte[bufSize + 4];

        ByteBuffer.wrap(res).order(ByteOrder.LITTLE_ENDIAN).putInt(binaryTupleSize);
        buf.get(res, 4, bufSize);

        return res;
    }

    /**
     * Deserializes streamer receiver info.
     *
     * @param bytes Bytes.
     * @param elementCount Number of elements in the binary tuple.
     * @return Streamer receiver info.
     */
    public static SteamerReceiverInfo deserializeReceiverInfo(ByteBuffer bytes, int elementCount) {
        var reader = new BinaryTupleReader(elementCount, bytes);

        int readerIndex = 0;
        String receiverClassName = reader.stringValue(readerIndex++);

        if (receiverClassName == null) {
            throw new IgniteException(PROTOCOL_ERR, "Receiver class name is null");
        }

        Object receiverArg = ClientBinaryTupleUtils.readObject(reader, readerIndex);

        readerIndex += 3;

        List<Object> items = readCollectionFromBinaryTuple(reader, readerIndex);

        return new SteamerReceiverInfo(receiverClassName, receiverArg, items);
    }

    /**
     * Serializes receiver results.
     *
     * @param receiverResults Receiver results.
     */
    public static byte @Nullable [] serializeReceiverJobResults(@Nullable List<Object> receiverResults) {
        if (receiverResults == null || receiverResults.isEmpty()) {
            return null;
        }

        int numElements = 2 + receiverResults.size();
        var builder = new BinaryTupleBuilder(numElements);
        appendCollectionToBinaryTuple(builder, receiverResults);

        ByteBuffer res = builder.build();

        // Resulting byte array.
        int numElementsSize = 4;
        byte[] resBytes = new byte[res.limit() - res.position() + numElementsSize];

        // Prepend count.
        ByteBuffer.wrap(resBytes).order(ByteOrder.LITTLE_ENDIAN).putInt(numElements);

        // Copy binary tuple.
        res.get(resBytes, numElementsSize, resBytes.length - numElementsSize);

        return resBytes;
    }

    /**
     * Deserializes receiver job results produced by {@link #serializeReceiverJobResults} method.
     *
     * @param results Serialized results.
     * @return Deserialized results.
     */
    public static <R> List<R> deserializeReceiverJobResults(byte[] results) {
        if (results == null || results.length == 0) {
            return List.of();
        }

        ByteBuffer buf = ByteBuffer.wrap(results).order(ByteOrder.LITTLE_ENDIAN);
        int numElements = buf.getInt();

        var reader = new BinaryTupleReader(numElements, buf.slice().order(ByteOrder.LITTLE_ENDIAN));

        return readCollectionFromBinaryTuple(reader, 0);
    }

    /**
     * Serializes receiver results.
     *
     * @param w Writer.
     * @param receiverJobResults Receiver results serialized by {@link #serializeReceiverJobResults}.
     */
    public static void serializeReceiverResultsForClient(ClientMessagePacker w, byte @Nullable [] receiverJobResults) {
        if (receiverJobResults == null || receiverJobResults.length == 0) {
            w.packNil();
            return;
        }

        int numElementsSize = 4;
        int binaryTupleSize = receiverJobResults.length - numElementsSize;

        int numElements = ByteBuffer.wrap(receiverJobResults).order(ByteOrder.LITTLE_ENDIAN).getInt();

        w.packInt(numElements);
        w.packBinaryHeader(binaryTupleSize);
        w.writePayload(receiverJobResults, numElementsSize, binaryTupleSize);
    }

    /**
     * Deserializes receiver results from {@link #serializeReceiverResultsForClient} method.
     *
     * @param r Reader.
     * @return Receiver results.
     */
    public static @Nullable <R> List<R> deserializeReceiverResultsOnClient(ClientMessageUnpacker r) {
        if (r.tryUnpackNil()) {
            return null;
        }

        int numElements = r.unpackInt();
        byte[] bytes = r.readBinary();
        var reader = new BinaryTupleReader(numElements, bytes);

        return readCollectionFromBinaryTuple(reader, 0);
    }

    /**
     * Packs an array of objects in BinaryTuple format.
     *
     * @param builder Target builder.
     * @param items Items.
     */
    private static <T> void appendCollectionToBinaryTuple(BinaryTupleBuilder builder, Collection<T> items) {
        assert items != null : "items can't be null";
        assert !items.isEmpty() : "items can't be empty";
        assert builder != null : "builder can't be null";

        T firstItem = items.iterator().next();
        Objects.requireNonNull(firstItem);
        Class<?> type = firstItem.getClass();

        Consumer<T> appender = appendTypeAndGetAppender(builder, firstItem);
        builder.appendInt(items.size());

        for (T item : items) {
            Objects.requireNonNull(item);
            if (!type.equals(item.getClass())) {
                throw new IllegalArgumentException(
                        "All items must have the same type. First item: " + type + ", current item: " + item.getClass());
            }

            appender.accept(item);
        }
    }

    private static <R> List<R> readCollectionFromBinaryTuple(BinaryTupleReader reader, int readerIndex) {
        int typeId = reader.intValue(readerIndex++);
        Function<Integer, Object> itemReader = readerForType(reader, typeId);
        int itemsCount = reader.intValue(readerIndex++);

        List<R> items = new ArrayList<>(itemsCount);
        for (int i = 0; i < itemsCount; i++) {
            items.add((R) itemReader.apply(readerIndex++));
        }

        return items;
    }

    /**
     * Writes type id to the specified packer and returns a consumer that writes the value to the binary tuple.
     *
     * @param builder Builder.
     * @param obj Object.
     */
    private static <T> Consumer<T> appendTypeAndGetAppender(BinaryTupleBuilder builder, Object obj) {
        assert obj != null : "Object is null";

        if (obj instanceof Boolean) {
            builder.appendInt(ColumnType.BOOLEAN.id());
            return (T v) -> builder.appendBoolean((Boolean) v);
        } else if (obj instanceof Byte) {
            builder.appendInt(ColumnType.INT8.id());
            return (T v) -> builder.appendByte((Byte) v);
        } else if (obj instanceof Short) {
            builder.appendInt(ColumnType.INT16.id());
            return (T v) -> builder.appendShort((Short) v);
        } else if (obj instanceof Integer) {
            builder.appendInt(ColumnType.INT32.id());
            return (T v) -> builder.appendInt((Integer) v);
        } else if (obj instanceof Long) {
            builder.appendInt(ColumnType.INT64.id());
            return (T v) -> builder.appendLong((Long) v);
        } else if (obj instanceof Float) {
            builder.appendInt(ColumnType.FLOAT.id());
            return (T v) -> builder.appendFloat((Float) v);
        } else if (obj instanceof Double) {
            builder.appendInt(ColumnType.DOUBLE.id());
            return (T v) -> builder.appendDouble((Double) v);
        } else if (obj instanceof BigDecimal) {
            builder.appendInt(ColumnType.DECIMAL.id());
            return (T v) -> builder.appendDecimal((BigDecimal) v, ((BigDecimal) v).scale());
        } else if (obj instanceof UUID) {
            builder.appendInt(ColumnType.UUID.id());
            return (T v) -> builder.appendUuid((UUID) v);
        } else if (obj instanceof String) {
            builder.appendInt(ColumnType.STRING.id());
            return (T v) -> builder.appendString((String) v);
        } else if (obj instanceof byte[]) {
            builder.appendInt(ColumnType.BYTE_ARRAY.id());
            return (T v) -> builder.appendBytes((byte[]) v);
        } else if (obj instanceof LocalDate) {
            builder.appendInt(ColumnType.DATE.id());
            return (T v) -> builder.appendDate((LocalDate) v);
        } else if (obj instanceof LocalTime) {
            builder.appendInt(ColumnType.TIME.id());
            return (T v) -> builder.appendTime((LocalTime) v);
        } else if (obj instanceof LocalDateTime) {
            builder.appendInt(ColumnType.DATETIME.id());
            return (T v) -> builder.appendDateTime((LocalDateTime) v);
        } else if (obj instanceof Instant) {
            builder.appendInt(ColumnType.TIMESTAMP.id());
            return (T v) -> builder.appendTimestamp((Instant) v);
        } else if (obj instanceof Duration) {
            builder.appendInt(ColumnType.DURATION.id());
            return (T v) -> builder.appendDuration((Duration) v);
        } else if (obj instanceof Period) {
            builder.appendInt(ColumnType.PERIOD.id());
            return (T v) -> builder.appendPeriod((Period) v);
        } else if (obj instanceof Tuple) {
            builder.appendInt(TYPE_ID_TUPLE);
            return (T v) -> builder.appendBytes(TupleWithSchemaMarshalling.marshal((Tuple) v));
        } else {
            throw unsupportedTypeException(obj.getClass());
        }
    }

    private static Function<Integer, Object> readerForType(BinaryTupleReader binTuple, int typeId) {
        if (typeId == TYPE_ID_TUPLE) {
            return idx -> {
                byte[] bytes = binTuple.bytesValue(idx);
                return bytes == null ? null : TupleWithSchemaMarshalling.unmarshal(bytes);
            };
        }

        ColumnType type = ColumnTypeConverter.fromIdOrThrow(typeId);

        switch (type) {
            case INT8:
                return binTuple::byteValue;

            case INT16:
                return binTuple::shortValue;

            case INT32:
                return binTuple::intValue;

            case INT64:
                return binTuple::longValue;

            case FLOAT:
                return binTuple::floatValue;

            case DOUBLE:
                return binTuple::doubleValue;

            case DECIMAL:
                return idx -> binTuple.decimalValue(idx, -1);

            case UUID:
                return binTuple::uuidValue;

            case STRING:
                return binTuple::stringValue;

            case BYTE_ARRAY:
                return binTuple::bytesValue;

            case DATE:
                return binTuple::dateValue;

            case TIME:
                return binTuple::timeValue;

            case DATETIME:
                return binTuple::dateTimeValue;

            case TIMESTAMP:
                return binTuple::timestampValue;

            case BOOLEAN:
                return idx -> binTuple.byteValue(idx) != 0;

            case DURATION:
                return binTuple::durationValue;

            case PERIOD:
                return binTuple::periodValue;

            default:
                throw unsupportedTypeException(type.id());
        }
    }

    /**
     * Streamer receiver info.
     */
    public static class SteamerReceiverInfo {
        private final String className;
        private final @Nullable Object arg;
        private final List<Object> items;

        private SteamerReceiverInfo(String className, @Nullable Object arg, List<Object> items) {
            this.className = className;
            this.arg = arg;
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
        public @Nullable Object arg() {
            return arg;
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
