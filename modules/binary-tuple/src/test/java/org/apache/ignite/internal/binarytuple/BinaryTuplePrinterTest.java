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

package org.apache.ignite.internal.binarytuple;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.internal.util.StringUtils;
import org.junit.jupiter.api.Test;

/** Tests for {@link BinaryTuplePrinter}. */
class BinaryTuplePrinterTest {

    @Test
    void printScalarFields() {
        StructNativeType schema = NativeTypes.structBuilder()
                .addField("id", NativeTypes.INT32, false)
                .addField("score", NativeTypes.INT64, false)
                .addField("flag", NativeTypes.BOOLEAN, false)
                .build();

        BinaryTuple tuple = new BinaryTuple(3,
                new BinaryTupleBuilder(3)
                        .appendInt(7)
                        .appendLong(42L)
                        .appendBoolean(true)
                        .build()
        );

        String result = new BinaryTuplePrinter(schema).print(tuple);

        assertEquals("BinaryTuple[id=7, score=42, flag=true]", result);
    }

    @Test
    void printStringIsQuoted() {
        StructNativeType schema = NativeTypes.structBuilder()
                .addField("name", NativeTypes.STRING, false)
                .build();

        BinaryTuple tuple = new BinaryTuple(1,
                new BinaryTupleBuilder(1).appendString("hello world").build()
        );

        assertEquals("BinaryTuple[name='hello world']", new BinaryTuplePrinter(schema).print(tuple));
    }

    @Test
    void printNullFieldRendersAsNull() {
        StructNativeType schema = NativeTypes.structBuilder()
                .addField("id", NativeTypes.INT32, false)
                .addField("name", NativeTypes.STRING, true)
                .build();

        BinaryTuple tuple = new BinaryTuple(2,
                new BinaryTupleBuilder(2).appendInt(1).appendNull().build()
        );

        assertEquals("BinaryTuple[id=1, name=null]", new BinaryTuplePrinter(schema).print(tuple));
    }

    @Test
    void printByteArrayRenderedAsHex() {
        StructNativeType schema = NativeTypes.structBuilder()
                .addField("data", NativeTypes.BYTES, false)
                .build();

        @SuppressWarnings("NumericCastThatLosesPrecision")
        byte[] bytes = {0x01, (byte) 0xAB, (byte) 0xFF};

        BinaryTuple tuple = new BinaryTuple(1,
                new BinaryTupleBuilder(1).appendBytes(bytes).build()
        );

        String expected = "BinaryTuple[data=" + StringUtils.toHexString(bytes) + "]";
        assertEquals(expected, new BinaryTuplePrinter(schema).print(tuple));
    }

    @Test
    void printDateTimeFields() {
        LocalDate date = LocalDate.of(2024, 6, 15);
        LocalTime time = LocalTime.of(10, 30, 0);
        LocalDateTime dateTime = LocalDateTime.of(date, time);

        StructNativeType schema = NativeTypes.structBuilder()
                .addField("d", NativeTypes.DATE, false)
                .addField("t", NativeTypes.time(0), false)
                .addField("dt", NativeTypes.datetime(0), false)
                .build();

        BinaryTuple tuple = new BinaryTuple(3,
                new BinaryTupleBuilder(3)
                        .appendDate(date)
                        .appendTime(time)
                        .appendDateTime(dateTime)
                        .build()
        );

        assertEquals(
                "BinaryTuple[d=" + date + ", t=" + time + ", dt=" + dateTime + "]",
                new BinaryTuplePrinter(schema).print(tuple)
        );
    }

    @Test
    void printByteBufferOverloadMatchesBinaryTupleOverload() {
        StructNativeType schema = NativeTypes.structBuilder()
                .addField("x", NativeTypes.INT32, false)
                .addField("y", NativeTypes.STRING, true)
                .build();

        ByteBuffer buf = new BinaryTupleBuilder(2).appendInt(99).appendString("test").build();
        BinaryTuple tuple = new BinaryTuple(2, buf);

        BinaryTuplePrinter printer = new BinaryTuplePrinter(schema);
        assertEquals(printer.print(tuple), printer.print(buf));
    }

    @Test
    void printEmptySchemaProducesEmptyBrackets() {
        StructNativeType schema = NativeTypes.structBuilder().build();

        BinaryTuple tuple = new BinaryTuple(0, new BinaryTupleBuilder(0).build());

        assertEquals("BinaryTuple[]", new BinaryTuplePrinter(schema).print(tuple));
    }
}
