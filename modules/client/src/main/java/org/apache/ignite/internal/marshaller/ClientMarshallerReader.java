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
        return unpacker.unpackBitSet();
    }

    @Override
    public BigInteger readBigInt() {
        return unpacker.unpackNumber();
    }

    @Override
    public BigDecimal readBigDecimal() {
        return unpacker.unpackDecimal();
    }

    @Override
    public LocalDate readDate() {
        return unpacker.unpackDate();
    }

    @Override
    public LocalTime readTime() {
        return unpacker.unpackTime();
    }

    @Override
    public Instant readTimestamp() {
        return unpacker.unpackTimestamp();
    }

    @Override
    public LocalDateTime readDateTime() {
        return unpacker.unpackDateTime();
    }
}
