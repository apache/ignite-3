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

package org.apache.ignite.client.proto;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.UUID;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.ignite.lang.IgniteException;
import org.msgpack.core.ExtensionTypeHeader;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageSizeException;
import org.msgpack.core.MessageTypeException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.InputStreamBufferInput;

import static org.apache.ignite.client.proto.ClientDataType.BITMASK;
import static org.apache.ignite.client.proto.ClientDataType.BYTES;
import static org.apache.ignite.client.proto.ClientDataType.DECIMAL;
import static org.apache.ignite.client.proto.ClientDataType.DOUBLE;
import static org.apache.ignite.client.proto.ClientDataType.FLOAT;
import static org.apache.ignite.client.proto.ClientDataType.INT16;
import static org.apache.ignite.client.proto.ClientDataType.INT32;
import static org.apache.ignite.client.proto.ClientDataType.INT64;
import static org.apache.ignite.client.proto.ClientDataType.INT8;
import static org.apache.ignite.client.proto.ClientDataType.STRING;

/**
 * Ignite-specific MsgPack extension based on Netty ByteBuf.
 * <p>
 * Releases wrapped buffer on {@link #close()} .
 */
public class ClientMessageUnpacker extends MessageUnpacker {
    /** Underlying buffer. */
    private final ByteBuf buf;

    /** Closed flag. */
    private boolean closed = false;

    /**
     * Constructor.
     *
     * @param buf Input.
     */
    public ClientMessageUnpacker(ByteBuf buf) {
        // TODO: Remove intermediate classes and buffers IGNITE-15234.
        super(new InputStreamBufferInput(new ByteBufInputStream(buf)), MessagePack.DEFAULT_UNPACKER_CONFIG);

        this.buf = buf;
    }


    /** {@inheritDoc} */
    @Override public int unpackInt() {
        try {
            return super.unpackInt();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String unpackString() {
        try {
            return super.unpackString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void unpackNil() {
        try {
            super.unpackNil();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean unpackBoolean() {
        try {
            return super.unpackBoolean();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte unpackByte() {
        try {
            return super.unpackByte();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public short unpackShort() {
        try {
            return super.unpackShort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long unpackLong() {
        try {
            return super.unpackLong();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public BigInteger unpackBigInteger() {
        try {
            return super.unpackBigInteger();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public float unpackFloat() {
        try {
            return super.unpackFloat();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public double unpackDouble() {
        try {
            return super.unpackDouble();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int unpackArrayHeader() {
        try {
            return super.unpackArrayHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int unpackMapHeader() {
        try {
            return super.unpackMapHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ExtensionTypeHeader unpackExtensionTypeHeader() {
        try {
            return super.unpackExtensionTypeHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int unpackBinaryHeader() {
        try {
            return super.unpackBinaryHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryUnpackNil() {
        try {
            return super.tryUnpackNil();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] readPayload(int length) {
        try {
            return super.readPayload(length);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessageFormat getNextFormat() {
        try {
            return super.getNextFormat();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void skipValue(int count) {
        try {
            super.skipValue(count);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void skipValue() {
        try {
            super.skipValue();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reads an UUID.
     *
     * @return UUID value.
     * @throws MessageTypeException when type is not UUID.
     * @throws MessageSizeException when size is not correct.
     */
    public UUID unpackUuid() {
        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.UUID)
            throw new MessageTypeException("Expected UUID extension (1), but got " + type);

        if (len != 16)
            throw new MessageSizeException("Expected 16 bytes for UUID extension, but got " + len, len);

        var bytes = readPayload(16);

        ByteBuffer bb = ByteBuffer.wrap(bytes);

        return new UUID(bb.getLong(), bb.getLong());
    }

    /**
     * Reads a decimal.
     *
     * @return Decimal value.
     */
    public BigDecimal unpackDecimal() {
        throw new UnsupportedOperationException("TODO: IGNITE-15163");
    }

    /**
     * Reads a bit set.
     *
     * @return Bit set.
     */
    public BitSet unpackBitSet() {
        throw new UnsupportedOperationException("TODO: IGNITE-15163");
    }

    /**
     * Unpacks an object based on the specified type.
     *
     * @param dataType Data type code.
     *
     * @return Unpacked object.
     * @throws IgniteException when data type is not valid.
     */
    public Object unpackObject(int dataType) {
        if (tryUnpackNil())
            return null;

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

    /** {@inheritDoc} */
    @Override public void close() {
        if (closed)
            return;

        closed = true;

        if (buf.refCnt() > 0)
            buf.release();
    }
}
