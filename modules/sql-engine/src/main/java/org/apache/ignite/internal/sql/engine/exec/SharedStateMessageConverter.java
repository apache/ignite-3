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

package org.apache.ignite.internal.sql.engine.exec;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.sql.engine.message.SharedStateMessage;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.sql.engine.message.field.SingleValueMessage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Converter between {@link SharedState} and {@link SharedStateMessage}.
 */
public class SharedStateMessageConverter {
    /** Message factory. */
    private static final SqlQueryMessagesFactory MESSAGE_FACTORY = new SqlQueryMessagesFactory();

    @Contract("null -> null; !null -> !null")
    static @Nullable SharedStateMessage toMessage(@Nullable SharedState state) {
        if (state == null) {
            return null;
        }

        Long2ObjectMap<Object> correlations = state.correlations();
        Map<Long, NetworkMessage> result = IgniteUtils.newHashMap(correlations.size());

        for (Long2ObjectMap.Entry<Object> entry : correlations.long2ObjectEntrySet()) {
            SingleValueMessage<?> msg = toSingleValueMessage(entry.getValue());

            result.put(entry.getLongKey(), msg);
        }

        return MESSAGE_FACTORY.sharedStateMessage()
                .sharedState(result)
                .build();
    }

    @Contract("null -> null; !null -> !null")
    static @Nullable SharedState fromMessage(@Nullable SharedStateMessage sharedStateMessage) {
        if (sharedStateMessage == null) {
            return null;
        }

        int size = sharedStateMessage.sharedState().size();
        Long2ObjectMap<Object> correlations = new Long2ObjectOpenHashMap<>(size);

        for (Map.Entry<Long, NetworkMessage> e : sharedStateMessage.sharedState().entrySet()) {
            NetworkMessage networkMessage = e.getValue();

            if (!(networkMessage instanceof SingleValueMessage)) {
                throw new IllegalArgumentException("Unexpected message type "
                        + "[type=" + networkMessage.messageType() + ", class=" + networkMessage.getClass() + ']');
            }

            SingleValueMessage<Object> singleFieldMessage = ((SingleValueMessage<Object>) networkMessage);

            correlations.put(e.getKey().longValue(), extractFieldValue(singleFieldMessage));
        }

        return new SharedState(correlations);
    }

    private static @Nullable Object extractFieldValue(SingleValueMessage<Object> msg) {
        Object value = msg.value();

        if (value == null) {
            return null;
        }

        switch (msg.messageType()) {
            case SqlQueryMessageGroup.BYTE_ARRAY_FIELD_MESSAGE:
                return new ByteString((byte[]) value);

            case SqlQueryMessageGroup.DECIMAL_FIELD_MESSAGE:
                return decimalFromBytes((byte[]) value);

            default:
                return value;
        }
    }

    private static BigDecimal decimalFromBytes(byte[] value) {
        ByteBuffer buffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN);

        short valScale = buffer.getShort();

        BigInteger integer = new BigInteger(value, Short.BYTES, value.length - Short.BYTES);

        return new BigDecimal(integer, valScale);
    }

    private static byte[] decimalToBytes(BigDecimal value) {
        if (value.scale() > Short.MAX_VALUE || value.scale() < Short.MIN_VALUE) {
            throw new UnsupportedOperationException("Decimal scale is out of range: " + value.scale());
        }

        byte[] unscaledBytes = value.unscaledValue().toByteArray();

        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES + unscaledBytes.length)
                .order(ByteOrder.LITTLE_ENDIAN);

        buffer.putShort((short) value.scale());
        buffer.put(unscaledBytes);

        return buffer.array();
    }

    private static SingleValueMessage<?> toSingleValueMessage(Object value) {
        if (value == null) {
            return MESSAGE_FACTORY.nullValueMessage().build();
        }

        if (value instanceof Boolean) {
            return MESSAGE_FACTORY.booleanValueMessage().value((Boolean) value).build();
        }
        if (value instanceof Byte) {
            return MESSAGE_FACTORY.byteValueMessage().value((Byte) value).build();
        }
        if (value instanceof Short) {
            return MESSAGE_FACTORY.shortValueMessage().value((Short) value).build();
        }
        if (value instanceof Integer) {
            return MESSAGE_FACTORY.intValueMessage().value((Integer) value).build();
        }
        if (value instanceof Long) {
            return MESSAGE_FACTORY.longValueMessage().value((Long) value).build();
        }
        if (value instanceof Float) {
            return MESSAGE_FACTORY.floatValueMessage().value((Float) value).build();
        }
        if (value instanceof Double) {
            return MESSAGE_FACTORY.doubleValueMessage().value((Double) value).build();
        }
        if (value instanceof BigDecimal) {
            return MESSAGE_FACTORY.decimalValueMessage().value(decimalToBytes((BigDecimal) value)).build();
        }
        if (value instanceof UUID) {
            return MESSAGE_FACTORY.uuidValueMessage().value((UUID) value).build();
        }
        if (value instanceof String) {
            return MESSAGE_FACTORY.stringValueMessage().value((String) value).build();
        }
        if (value instanceof ByteString) {
            ByteString byteString = (ByteString) value;

            return MESSAGE_FACTORY.byteArrayValueMessage().value(byteString.getBytes()).build();
        }

        throw new IllegalArgumentException("Unsupported type: " + value.getClass());
    }
}
