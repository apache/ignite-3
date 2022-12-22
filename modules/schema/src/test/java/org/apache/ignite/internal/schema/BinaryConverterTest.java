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

package org.apache.ignite.internal.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.reflection.RecordMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

class BinaryConverterTest {
    private static final SchemaDescriptor SCHEMA;
    private static final BinaryConverter CONVERTER;

    static {
        NativeType[] types = {
                NativeTypes.INT8, NativeTypes.INT16, NativeTypes.INT32, NativeTypes.INT64,
                NativeTypes.FLOAT, NativeTypes.DOUBLE, NativeTypes.UUID, NativeTypes.STRING,
                NativeTypes.BYTES, NativeTypes.DATE, NativeTypes.time(), NativeTypes.timestamp(), NativeTypes.datetime(),
                NativeTypes.numberOf(2), NativeTypes.decimalOf(5, 2), NativeTypes.bitmaskOf(8)
        };

        List<Column> valueCols = new ArrayList<>(types.length * 2);

        for (NativeType type : types) {
            String colName = "F" + type.spec().name().toUpperCase();

            valueCols.add(new Column(colName, type, false));
            valueCols.add(new Column(colName + "N", type, true));
        }

        SCHEMA = new SchemaDescriptor(1,
                new Column[]{new Column("ID", NativeTypes.INT64, false)},
                valueCols.toArray(Column[]::new)
        );

        CONVERTER = BinaryConverter.forRow(SCHEMA);
    }

    @Test
    public void testNonnulls() throws MarshallerException {
        Value row = new Value(1, false);
        Value converted = convertToTupleAndBack(row);
        assertEquals(row, converted);
    }

    @Test
    public void testNulls() throws MarshallerException {
        Value row = new Value(1, false);
        Value converted = convertToTupleAndBack(row);
        assertEquals(row, converted);
    }

    private static Value convertToTupleAndBack(Value row) throws MarshallerException {
        RecordMarshallerImpl<Value> marshaller = new RecordMarshallerImpl<>(SCHEMA, Mapper.of(Value.class));
        BinaryTuple tuple = CONVERTER.toTuple(marshaller.marshal(row));
        BinaryRow binaryRow = CONVERTER.fromTuple(tuple.byteBuffer());
        return marshaller.unmarshal(new Row(SCHEMA, binaryRow));
    }

    private static class Value {
        private long id;
        private byte fint8;
        private Byte fint8N;
        private short fint16;
        private Short fint16N;
        private int fint32;
        private Integer fint32N;
        private long fint64;
        private Long fint64N;
        private float ffloat;
        private Float ffloatN;
        private double fdouble;
        private Double fdoubleN;
        private UUID fuuid;
        private UUID fuuidN;
        private String fstring;
        private String fstringN;
        private byte[] fbytes;
        private byte[] fbytesN;
        private LocalDate fdate;
        private LocalDate fdateN;
        private LocalTime ftime;
        private LocalTime ftimeN;
        private LocalDateTime fdatetime;
        private LocalDateTime fdatetimeN;
        private Instant ftimestamp;
        private Instant ftimestampN;
        private BigInteger fnumber;
        private BigInteger fnumberN;
        private BigDecimal fdecimal;
        private BigDecimal fdecimalN;
        private BitSet fbitmask;
        private BitSet fbitmaskN;

        public Value() {
        }

        public Value(int id, boolean nulls) {
            this.id = id;
            fint8 = (byte) id;
            fint8N = (nulls) ? Byte.valueOf((byte) id) : null;
            fint16 = (short) id;
            fint16N = (nulls) ? Short.valueOf((short) id) : null;
            fint32 = id;
            fint32N = (nulls) ? id : null;
            fint64 = id;
            fint64N = (nulls) ? (long) id : null;
            ffloat = id;
            ffloatN = (nulls) ? Float.valueOf(id) : null;
            fdouble = id;
            fdoubleN = (nulls) ? Double.valueOf(id) : null;

            fuuid = new UUID(0L, (long) id);
            fuuidN = (nulls) ? fuuid : null;

            fstring = String.valueOf(id);
            fstringN = (nulls) ? String.valueOf(id) : null;

            fbytes = String.valueOf(id).getBytes(StandardCharsets.UTF_8);
            fbytesN = (nulls) ? String.valueOf(id).getBytes(StandardCharsets.UTF_8) : null;

            fdate = LocalDate.ofYearDay(2021, id);
            fdateN = (nulls) ? LocalDate.ofYearDay(2021, id) : null;
            ftime = LocalTime.ofSecondOfDay(id);
            ftimeN = (nulls) ? LocalTime.ofSecondOfDay(id) : null;
            fdatetime = LocalDateTime.ofEpochSecond(id, 0, ZoneOffset.UTC);
            fdatetimeN = (nulls) ? LocalDateTime.ofEpochSecond(id, 0, ZoneOffset.UTC) : null;
            ftimestamp = Instant.ofEpochSecond(id);
            ftimestampN = (nulls) ? Instant.ofEpochSecond(id) : null;
            fnumber = BigInteger.valueOf(id);
            fnumberN = (nulls) ? BigInteger.valueOf(id) : null;
            new BigDecimal(fnumber, 2);
            fdecimal = BigDecimal.valueOf(id * 100).movePointLeft(2);
            fdecimalN = (nulls) ? BigDecimal.valueOf(id * 100).movePointLeft(2) : null;
            fbitmask = new BitSet();
            fbitmask.set(id);
            if (nulls) {
                fbitmaskN = new BitSet();
                fbitmaskN.set(id);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Value row = (Value) o;
            return id == row.id && fint8 == row.fint8 && fint16 == row.fint16 && fint32 == row.fint32 && fint64 == row.fint64
                    && Float.compare(row.ffloat, ffloat) == 0 && Double.compare(row.fdouble, fdouble) == 0
                    && Objects.equals(fint8N, row.fint8N) && Objects.equals(fint16N, row.fint16N) && Objects.equals(
                    fint32N, row.fint32N) && Objects.equals(fint64N, row.fint64N) && Objects.equals(ffloatN, row.ffloatN)
                    && Objects.equals(fdoubleN, row.fdoubleN) && Objects.equals(fuuid, row.fuuid) && Objects.equals(
                    fuuidN, row.fuuidN) && Objects.equals(fstring, row.fstring) && Objects.equals(fstringN, row.fstringN)
                    && Arrays.equals(fbytes, row.fbytes) && Arrays.equals(fbytesN, row.fbytesN) && Objects.equals(
                    fdate, row.fdate) && Objects.equals(fdateN, row.fdateN) && Objects.equals(ftime, row.ftime)
                    && Objects.equals(ftimeN, row.ftimeN) && Objects.equals(fdatetime, row.fdatetime)
                    && Objects.equals(fdatetimeN, row.fdatetimeN) && Objects.equals(ftimestamp, row.ftimestamp)
                    && Objects.equals(ftimestampN, row.ftimestampN) && Objects.equals(fnumber, row.fnumber)
                    && Objects.equals(fnumberN, row.fnumberN) && Objects.equals(fdecimal, row.fdecimal)
                    && Objects.equals(fdecimalN, row.fdecimalN) && Objects.equals(fbitmask, row.fbitmask)
                    && Objects.equals(fbitmaskN, row.fbitmaskN);
        }
    }
}
