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
        packer.packBitSet(val);
    }

    @Override
    public void writeBigInt(BigInteger val) {
        packer.packNumber(val);
    }

    @Override
    public void writeBigDecimal(BigDecimal val) {
        packer.packDecimal(val);
    }

    @Override
    public void writeDate(LocalDate val) {
        packer.packDate(val);
    }

    @Override
    public void writeTime(LocalTime val) {
        packer.packTime(val);
    }

    @Override
    public void writeTimestamp(Instant val) {
        packer.packTimestamp(val);
    }

    @Override
    public void writeDateTime(LocalDateTime val) {
        packer.packDateTime(val);
    }
}
