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

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import org.apache.ignite.internal.util.VarIntUtils;

/**
 * Protocol-wide elements marshalling.
 */
class ProtocolMarshalling {
    /** Maximum number of bytes needed to encode a container length. */
    static final int MAX_LENGTH_BYTE_COUNT = 4;

    static void writeDescriptorOrCommandId(int id, DataOutput output) throws IOException {
        writeUnsignedVarInt(id, output);
    }

    private static void writeUnsignedVarInt(int val, DataOutput output) throws IOException {
        assert val >= 0 : val;

        // We subtract 1 because VarIntUtils#writeVarInt() adds 1. It does so to effectively pass a frequenly used -1, but we don't need
        // it here as the values are always unsigned.
        VarIntUtils.writeVarInt(val - 1, output);
    }

    static int readDescriptorOrCommandId(DataInput input) throws IOException {
        return readUnsignedVarInt(input);
    }

    private static int readUnsignedVarInt(DataInput input) throws IOException {
        long value = VarIntUtils.readVarInt(input);

        //noinspection NumericCastThatLosesPrecision
        assert value >= -1 && (int) value == value : value;

        // We add 1 because #readUnsignedVarInt() subtracts 1.
        return (int) value + 1;
    }

    static void writeObjectId(int id, DataOutput output) throws IOException {
        writeUnsignedVarInt(id, output);
    }

    static int readObjectId(DataInput input) throws IOException {
        return readUnsignedVarInt(input);
    }

    static void writeLength(int length, DataOutput output) throws IOException {
        writeUnsignedVarInt(length, output);
    }

    static int readLength(DataInput input) throws IOException {
        return readUnsignedVarInt(input);
    }

    static void writeFixedLengthBitSet(BitSet bitset, int bitSetLength, DataOutput output) throws IOException {
        byte bits = 0;
        int writtenBytes = 0;
        for (int i = 0; i < bitSetLength; i++) {
            boolean bit = bitset.get(i);
            int bitIndex = i % 8;
            if (bit) {
                bits |= (1 << bitIndex);
            }
            if (bitIndex == 7) {
                output.writeByte(bits);
                writtenBytes++;
                bits = 0;
            }
        }

        int totalBytesToWrite = bitSetLength / 8 + (bitSetLength % 8 == 0 ? 0 : 1);
        if (writtenBytes < totalBytesToWrite) {
            output.writeByte(bits);
        }
    }

    static BitSet readFixedLengthBitSet(int bitSetLength, DataInput input) throws IOException {
        BitSet bitSet = new BitSet(bitSetLength);

        int totalBytesToRead = bitSetLength / 8 + (bitSetLength % 8 == 0 ? 0 : 1);

        for (int byteIndex = 0; byteIndex < totalBytesToRead; byteIndex++) {
            byte bits = input.readByte();

            int bitsToReadInThisByte;
            if (byteIndex < totalBytesToRead - 1) {
                bitsToReadInThisByte = 8;
            } else {
                bitsToReadInThisByte = bitSetLength - (totalBytesToRead - 1) * 8;
            }
            for (int bitIndex = 0; bitIndex < bitsToReadInThisByte; bitIndex++) {
                if ((bits & (1 << bitIndex)) != 0) {
                    bitSet.set(byteIndex * 8 + bitIndex);
                }
            }
        }

        return bitSet;
    }

    private ProtocolMarshalling() {
    }
}
