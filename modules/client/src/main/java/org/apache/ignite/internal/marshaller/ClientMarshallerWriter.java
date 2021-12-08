package org.apache.ignite.internal.marshaller;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;

public class ClientMarshallerWriter implements MarshallerWriter {
    /** */
    private final ClientMessagePacker packer;

    public ClientMarshallerWriter(ClientMessagePacker packer) {
        this.packer = packer;
    }

    @Override
    public void writeNull() {
        packer.packNil();
    }

    @Override
    public void writeByte(byte val) {
        packer.packByte(val);
    }

    @Override
    public void writeShort(short val) {
        packer.packShort(val);
    }

    @Override
    public void writeInt(int val) {
        packer.packInt(val);
    }

    @Override
    public void writeLong(long val) {
        packer.packLong(val);
    }

    @Override
    public void writeFloat(float val) {
        packer.packFloat(val);
    }

    @Override
    public void writeDouble(double val) {
        packer.packDouble(val);
    }

    @Override
    public void writeString(String val) {
        packer.packString(val);
    }

    @Override
    public void writeUuid(UUID val) {
        packer.packUuid(val);
    }

    @Override
    public void writeBytes(byte[] val) {
        packer.packBinaryHeader(val.length);
        packer.writePayload(val);
    }

    @Override
    public void writeBitSet(BitSet val) {

    }

    @Override
    public void writeBigInt(BigInteger val) {
        packer.packNumber(val);
    }

    @Override
    public void writeBigDecimal(BigDecimal val) {
    }

    @Override
    public void writeDate(LocalDate val) {

    }

    @Override
    public void writeTime(LocalTime val) {

    }

    @Override
    public void writeTimestamp(Instant val) {

    }

    @Override
    public void writeDateTime(LocalDateTime val) {

    }
}
