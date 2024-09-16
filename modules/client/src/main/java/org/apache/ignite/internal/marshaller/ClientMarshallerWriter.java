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

package org.apache.ignite.internal.marshaller;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;

/**
 * Binary writer over {@link ClientMessagePacker}.
 */
public class ClientMarshallerWriter implements MarshallerWriter {
    /** Packer. */
    private final BinaryTupleBuilder packer;

    /** No-value bit set. */
    private final BitSet noValueSet;

    /**
     * Constructor.
     *
     * @param packer Packer.
     */
    public ClientMarshallerWriter(BinaryTupleBuilder packer, BitSet noValueSet) {
        this.packer = packer;
        this.noValueSet = noValueSet;
    }

    /** {@inheritDoc} */
    @Override
    public void writeNull() {
        packer.appendNull();
    }

    /** {@inheritDoc} */
    @Override
    public void writeAbsentValue() {
        noValueSet.set(packer.elementIndex());
        packer.appendNull();
    }

    /** {@inheritDoc} */
    @Override
    public void writeBoolean(boolean val) {
        packer.appendBoolean(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeByte(byte val) {
        packer.appendByte(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeShort(short val) {
        packer.appendShort(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeInt(int val) {
        packer.appendInt(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeLong(long val) {
        packer.appendLong(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeFloat(float val) {
        packer.appendFloat(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDouble(double val) {
        packer.appendDouble(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeString(String val) {
        packer.appendString(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeUuid(UUID val) {
        packer.appendUuid(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBytes(byte[] val) {
        packer.appendBytes(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBigDecimal(BigDecimal val, int scale) {
        packer.appendDecimal(val, scale);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDate(LocalDate val) {
        packer.appendDate(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeTime(LocalTime val) {
        packer.appendTime(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeTimestamp(Instant val) {
        packer.appendTimestamp(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDateTime(LocalDateTime val) {
        packer.appendDateTime(val);
    }
}
