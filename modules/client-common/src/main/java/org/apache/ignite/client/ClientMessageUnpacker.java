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

package org.apache.ignite.client;

import org.apache.ignite.lang.IgniteException;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageSizeException;
import org.msgpack.core.MessageTypeException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.MessageBufferInput;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;

import static org.apache.ignite.client.ClientDataType.BITMASK;
import static org.apache.ignite.client.ClientDataType.BYTES;
import static org.apache.ignite.client.ClientDataType.DECIMAL;
import static org.apache.ignite.client.ClientDataType.DOUBLE;
import static org.apache.ignite.client.ClientDataType.FLOAT;
import static org.apache.ignite.client.ClientDataType.INT16;
import static org.apache.ignite.client.ClientDataType.INT32;
import static org.apache.ignite.client.ClientDataType.INT64;
import static org.apache.ignite.client.ClientDataType.INT8;
import static org.apache.ignite.client.ClientDataType.STRING;

/**
 * Ignite-specific MsgPack extension.
 */
public class ClientMessageUnpacker extends MessageUnpacker {
    /**
     * Create an MessageUnpacker that reads data from the given MessageBufferInput.
     * This method is available for subclasses to override. Use MessagePack.UnpackerConfig.newUnpacker method to instantiate this implementation.
     *
     * @param in Input.
     */
    public ClientMessageUnpacker(MessageBufferInput in) {
        super(in, MessagePack.DEFAULT_UNPACKER_CONFIG);
    }

    public UUID unpackUuid() throws IOException {
        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.UUID)
            throw new MessageTypeException("Expected UUID extension (1), but got " + type);

        if (len != 16)
            throw new MessageSizeException("Expected 16 bytes for UUID extension, but got " + len, len);

        var bytes = readPayload(16);

        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.order(ByteOrder.BIG_ENDIAN);

        return new UUID(bb.getLong(), bb.getLong());
    }

    public BigDecimal unpackDecimal() throws IOException {
        throw new IgniteException("TODO");
    }

    public BitSet unpackBitSet() throws IOException {
        throw new IgniteException("TODO");
    }

    public Object unpackObject() throws IOException {
        MessageFormat format = getNextFormat();

        switch (format) {
            case POSFIXINT:
            case NEGFIXINT:
            case INT8:
                return unpackByte();

            case NIL:
                return null;

            case BOOLEAN:
                return unpackBoolean();

            case UINT8:
            case INT16:
                return unpackShort();

            case UINT16:
            case INT32:
                return unpackInt();

            case UINT32:
            case UINT64:
            case INT64:
                return unpackLong();

            case FIXSTR:
            case STR8:
            case STR16:
            case STR32:
                return unpackString();
        }

        // TODO: Support all basic types.
        throw new IgniteException("Unsupported type, can't deserialize: " + format);
    }

    public Object unpackObject(int dataType) throws IOException {
        switch (dataType) {
            case INT8:
                return unpackByte();

            case INT16:
                return unpackShort();

            case INT32:
                return unpackInt();

            case INT64:
                return unpackLong();

            case FLOAT:
                return unpackFloat();

            case DOUBLE:
                return unpackDouble();

            case ClientDataType.UUID:
                return unpackUuid();

            case STRING:
                return unpackString();

            case BYTES:
                var cnt = unpackBinaryHeader();

                return readPayload(cnt);

            case DECIMAL:
                return unpackDecimal();

            case BITMASK:
                return unpackBitSet();
        }

        throw new IgniteException("Unknown client data type: " + dataType);
    }
}
