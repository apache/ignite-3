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
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.buffer.ArrayBufferOutput;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * Ignite-specific MsgPack extension.
 */
public class ClientMessagePacker extends MessageBufferPacker {
    public ClientMessagePacker() {
        super(new ArrayBufferOutput(), MessagePack.DEFAULT_PACKER_CONFIG);
    }

    public ClientMessagePacker packUuid(UUID v) throws IOException {
        packExtensionTypeHeader(ClientMsgPackType.UUID, 16);

        var bytes = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.order(ByteOrder.BIG_ENDIAN);

        bb.putLong(v.getMostSignificantBits());
        bb.putLong(v.getLeastSignificantBits());

        writePayload(bytes);

        return this;
    }

    public ClientMessagePacker packDecimal(BigDecimal v) throws IOException {
        throw new IOException("TODO");
    }

    public ClientMessagePacker packObject(Object val) throws IOException {
        if (val == null)
            return (ClientMessagePacker) packNil();

        if (val instanceof Integer)
            return (ClientMessagePacker) packInt((int) val);

        if (val instanceof String)
            return (ClientMessagePacker) packString((String) val);

        if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            packBinaryHeader(bytes.length);
            writePayload(bytes);

            return this;
        }

        throw new IgniteException("TODO: Support all types");
    }
}
