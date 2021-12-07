package org.apache.ignite.internal.marshaller;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;

public class ClientMarshallerReader implements MarshallerReader {
    /** */
    private final ClientMessageUnpacker unpacker;

    public ClientMarshallerReader(ClientMessageUnpacker unpacker) {
        this.unpacker = unpacker;
    }

    @Override
    public void skipValue() {
        unpacker.skipValues(1);
    }

    @Override
    public byte readByte() {
        return unpacker.unpackByte();
    }

    @Override
    public short readShort() {
        return unpacker.unpackShort();
    }

    @Override
    public int readInt() {
        return unpacker.unpackInt();
    }

    @Override
    public long readLong() {
        return unpacker.unpackLong();
    }

    @Override
    public float readFloat() {
        return unpacker.unpackFloat();
    }

    @Override
    public double readDouble() {
        return unpacker.unpackDouble();
    }

    @Override
    public String readString() {
        return unpacker.unpackString();
    }

    @Override
    public UUID readUuid() {
        return unpacker.unpackUuid();
    }

    @Override
    public byte[] readBytes() {
        return unpacker.readPayload(unpacker.unpackBinaryHeader());
    }

    @Override
    public BitSet readBitSet() {
        return null;
    }

    @Override
    public BigInteger readBigInt() {
        return null;
    }

    @Override
    public BigDecimal readBigDecimal() {
        return null;
    }

    @Override
    public LocalDate readDate() {
        return null;
    }

    @Override
    public LocalTime readTime() {
        return null;
    }

    @Override
    public Instant readTimestamp() {
        return null;
    }

    @Override
    public LocalDateTime readDateTime() {
        return null;
    }
}
