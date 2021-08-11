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
import io.netty.buffer.ByteBufOutputStream;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.OutputStreamBufferOutput;
import org.msgpack.value.Value;

import static org.apache.ignite.client.proto.ClientMessageCommon.HEADER_SIZE;

/**
 * Ignite-specific MsgPack extension based on Netty ByteBuf.
 * <p>
 * Releases wrapped buffer on {@link #close()} .
 */
public class ClientMessagePacker extends MessagePacker {
    /** Underlying buffer. */
    private final ByteBuf buf;

    /** Closed flag. */
    private boolean closed = false;

    /**
     * Constructor.
     *
     * @param buf Buffer.
     */
    public ClientMessagePacker(ByteBuf buf) {
        // TODO: Remove intermediate classes and buffers IGNITE-15234.
        // Reserve 4 bytes for the message length.
        super(new OutputStreamBufferOutput(new ByteBufOutputStream(buf.writerIndex(HEADER_SIZE))),
                MessagePack.DEFAULT_PACKER_CONFIG);

        this.buf = buf;
    }

    /**
     * Gets the underlying buffer.
     *
     * @return Underlying buffer.
     * @throws UncheckedIOException When flush fails.
     */
    public ByteBuf getBuffer() {
        try {
            flush();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        buf.setInt(0, buf.writerIndex() - HEADER_SIZE);

        return buf;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packNil() {
        assert !closed : "Packer is closed";

        try {
            return super.packNil();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packBoolean(boolean b) {
        assert !closed : "Packer is closed";

        try {
            return super.packBoolean(b);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packByte(byte b) {
        assert !closed : "Packer is closed";

        try {
            return super.packByte(b);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packShort(short v) {
        assert !closed : "Packer is closed";

        try {
            return super.packShort(v);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packInt(int r) {
        assert !closed : "Packer is closed";

        try {
            return super.packInt(r);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packLong(long v) {
        assert !closed : "Packer is closed";

        try {
            return super.packLong(v);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packBigInteger(BigInteger bi) {
        assert !closed : "Packer is closed";

        try {
            return super.packBigInteger(bi);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packFloat(float v) {
        assert !closed : "Packer is closed";

        try {
            return super.packFloat(v);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packDouble(double v) {
        assert !closed : "Packer is closed";

        try {
            return super.packDouble(v);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packString(String s) {
        assert !closed : "Packer is closed";

        try {
            return super.packString(s);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packArrayHeader(int arraySize) {
        assert !closed : "Packer is closed";

        try {
            return super.packArrayHeader(arraySize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packMapHeader(int mapSize) {
        assert !closed : "Packer is closed";

        try {
            return super.packMapHeader(mapSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packValue(Value v) {
        assert !closed : "Packer is closed";

        try {
            return super.packValue(v);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packExtensionTypeHeader(byte extType, int payloadLen) {
        assert !closed : "Packer is closed";

        try {
            return super.packExtensionTypeHeader(extType, payloadLen);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packBinaryHeader(int len) {
        assert !closed : "Packer is closed";

        try {
            return super.packBinaryHeader(len);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packRawStringHeader(int len) {
        assert !closed : "Packer is closed";

        try {
            return super.packRawStringHeader(len);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker writePayload(byte[] src) {
        assert !closed : "Packer is closed";

        try {
            return super.writePayload(src);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker writePayload(byte[] src, int off, int len) {
        assert !closed : "Packer is closed";

        try {
            return super.writePayload(src, off, len);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker addPayload(byte[] src) {
        assert !closed : "Packer is closed";

        try {
            return super.addPayload(src);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker addPayload(byte[] src, int off, int len) {
        assert !closed : "Packer is closed";

        try {
            return super.addPayload(src, off, len);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes an UUID.
     *
     * @param val UUID value.
     * @return This instance.
     */
    public ClientMessagePacker packUuid(UUID val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.UUID, 16);

        // TODO: Pack directly to ByteBuf without allocating IGNITE-15234.
        var bytes = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(bytes);

        bb.putLong(val.getMostSignificantBits());
        bb.putLong(val.getLeastSignificantBits());

        writePayload(bytes);

        return this;
    }

    /**
     * Writes a decimal.
     *
     * @param val Decimal value.
     * @return This instance.
     * @throws UnsupportedOperationException Not supported.
     */
    public ClientMessagePacker packDecimal(BigDecimal val) {
        assert !closed : "Packer is closed";

        throw new UnsupportedOperationException("TODO: IGNITE-15163");
    }

    /**
     * Writes a bit set.
     *
     * @param val Bit set value.
     * @return This instance.
     * @throws UnsupportedOperationException Not supported.
     */
    public ClientMessagePacker packBitSet(BitSet val) {
        assert !closed : "Packer is closed";

        throw new UnsupportedOperationException("TODO: IGNITE-15163");
    }

    /**
     * Packs an object.
     *
     * @param val Object value.
     * @return This instance.
     * @throws UnsupportedOperationException When type is not supported.
     */
    public ClientMessagePacker packObject(Object val) {
        if (val == null)
            return (ClientMessagePacker) packNil();

        if (val instanceof Integer)
            return (ClientMessagePacker) packInt((int) val);

        if (val instanceof Long)
            return (ClientMessagePacker) packLong((long) val);

        if (val instanceof UUID)
            return packUuid((UUID) val);

        if (val instanceof String)
            return (ClientMessagePacker) packString((String) val);

        if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            packBinaryHeader(bytes.length);
            writePayload(bytes);

            return this;
        }

        if (val instanceof BigDecimal)
            return packDecimal((BigDecimal) val);

        if (val instanceof BitSet)
            return packBitSet((BitSet) val);

        // TODO: Support all basic types IGNITE-15163
        throw new UnsupportedOperationException("Unsupported type, can't serialize: " + val.getClass());
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
