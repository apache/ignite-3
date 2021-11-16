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

package org.apache.ignite.internal.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for different access methods:
 * 1) Create single table.
 * 2) Write throw different API's into it (row 1 - with all values, row 2 - with nulls).
 * 3) Read data back through all possible APIs and validate it.
 */
public class InteropOperationsTest {
    /** Test schema. */
    private static final SchemaDescriptor SCHEMA;

    /** Table for tests. */
    private static final TableImpl TABLE;

    /** Dummy internal table for tests. */
    private static final InternalTable INT_TABLE;

    /** Key value binary view for test. */
    private static final KeyValueView<Tuple, Tuple> KV_BIN_VIEW;

    /** Key value view for test. */
    private static final KeyValueView<Long, Value> KV_VIEW;

    /** Record view for test. */
    private static final RecordView<Row> R_VIEW;

    /** Record binary view for test. */
    private static final RecordView<Tuple> R_BIN_VIEW;

    static {
        NativeType[] types = {NativeTypes.INT8, NativeTypes.INT16, NativeTypes.INT32, NativeTypes.INT64,
            NativeTypes.FLOAT, NativeTypes.DOUBLE, NativeTypes.UUID, NativeTypes.STRING, NativeTypes.BYTES};

        List<Column> valueCols = new ArrayList<>(types.length * 2);

        for (NativeType type : types) {
            String colName = "f" + type.spec().name().toLowerCase();

            valueCols.add(new Column(colName, type, false));
            valueCols.add(new Column(colName + "N", type, true));
        }
        SCHEMA = new SchemaDescriptor(1,
                new Column[]{new Column("id", NativeTypes.INT64, false)},
                valueCols.toArray(Column[]::new)
        );


        INT_TABLE = new DummyInternalTableImpl();
        SchemaRegistry schemaRegistry = new DummySchemaManagerImpl(SCHEMA);

        TABLE = new TableImpl(INT_TABLE, schemaRegistry, null);
        KV_BIN_VIEW =  new KeyValueBinaryViewImpl(INT_TABLE, schemaRegistry, null, null);

        KV_VIEW = new KeyValueViewImpl<Long, Value>(INT_TABLE, schemaRegistry,
                Mapper.identity(Long.class), Mapper.identity(Value.class), null);

        R_BIN_VIEW = TABLE.recordView();
        R_VIEW = TABLE.recordView(Mapper.identity(Row.class));
    }

    @AfterEach
    public void clearTable() {
        TABLE.recordView().delete(Tuple.create().set("id", 1L));
        TABLE.recordView().delete(Tuple.create().set("id", 2L));
    }

    /**
     * Write through key value API and test records.
     */
    @Test
    public void keyValueWriteTest() {
        writeKewVal(1, false);
        writeKewVal(2, true);

        readback();
    }

    /**
     * Write through key value binary API and test records.
     */
    @Test
    public void keyValueBinaryWriteTest() {
        writeKeyValueBinary(1, false);
        writeKeyValueBinary(2, true);

        readback();
    }

    /**
     * Write through record API and test records.
     */
    @Test
    public void recordWriteTest() {
        writeRecord(1, false);
        writeRecord(2, true);

        readback();
    }

    /**
     * Write through record binary API and test records.
     */
    @Test
    public void recordBinaryWriteTest() {
        writeRecordBinary(1, false);
        writeRecordBinary(2, true);

        readback();
    }

    /**
     * Read back records through all APIs.
     */
    private void readback() {
        assertTrue(readKeyValue(1, false));
        assertTrue(readKeyValue(2, true));
        assertFalse(readKeyValue(0, false));

        assertTrue(readKeyValueBinary(1, false));
        assertTrue(readKeyValueBinary(2, true));
        assertFalse(readKeyValueBinary(0, false));

        assertTrue(readRecord(1, false));
        assertTrue(readRecord(2, true));
        assertFalse(readRecord(0, false));

        assertTrue(readRecordBinary(1, false));
        assertTrue(readRecordBinary(2, true));
        assertFalse(readRecordBinary(0, false));
    }

    /**
     * Write through binary view.
     *
     * @param id Id to write.
     * @param nulls If {@code true} - nullable fields will be filled, if {@code false} - otherwise.
     */
    private void writeKeyValueBinary(int id, boolean nulls) {
        Tuple k = Tuple.create().set("id", (long) id);
        Tuple v = createTuple(id, nulls);

        KV_BIN_VIEW.put(k, v);
    }

    /**
     * Read through binary view.
     *
     * @param id Id to read.
     * @return {@code true} if read successfully, {@code false} - otherwise.
     */
    private boolean readKeyValueBinary(int id, boolean nulls) {
        Tuple k = Tuple.create().set("id", (long) id);

        Tuple v = KV_BIN_VIEW.get(k);
        boolean contains = KV_BIN_VIEW.contains(k);

        assertEquals((v != null), contains);

        if (v == null) {
            return false;
        }

        v.set("id", (long) id);

        validateTuple(id, v, nulls);

        return true;
    }

    /**
     * Write through binary view.
     *
     * @param id Id to write.
     * @param nulls If {@code true} - nullable fields will be filled, if {@code false} - otherwise.
     */
    private void writeRecord(int id, boolean nulls) {
        Row r1 = new Row(id, nulls);

        assertTrue(R_VIEW.insert(r1));
    }

    /**
     * Read through record view.
     *
     * @param id Id to read.
     * @param nulls If {@code true} - nullable fields should be filled, if {@code false} - otherwise.
     * @return {@code true} if read successfully, {@code false} - otherwise.
     */
    private boolean readRecord(int id, boolean nulls) {
        Row expected = new Row(id, nulls);

        Row actual = R_VIEW.get(expected);

        if (actual == null) {
            return false;
        }

        assertEquals(expected, actual);

        return true;
    }

    /**
     * Write through record binary  view.
     *
     * @param id Id to write.
     * @param nulls If {@code true} - nullable fields will be filled, if {@code false} - otherwise.
     */
    private void writeRecordBinary(int id, boolean nulls) {
        Tuple t1 = createTuple(id, nulls);
        t1.set("id", (long) id);

        assertTrue(R_BIN_VIEW.insert(t1));
    }

    /**
     * Read through record binary view.
     *
     * @param id Id to read.
     * @param nulls If {@code true} - nullable fields should be filled, if {@code false} - otherwise.
     * @return {@code true} if read successfully, {@code false} - otherwise.
     */
    private boolean readRecordBinary(int id, boolean nulls) {
        Tuple k = Tuple.create().set("id", (long) id);

        Tuple res = R_BIN_VIEW.get(k);

        if (res == null) {
            return false;
        }

        validateTuple(id, res, nulls);

        return true;
    }

    /**
     * Write through binary view.
     *
     * @param id Id to write.
     * @param nulls If {@code true} - nullable fields will be filled, if {@code false} - otherwise.
     */
    private void writeKewVal(int id, boolean nulls) {
        KV_VIEW.put((long) id, new Value(id, nulls));
    }

    /**
     * Read through binary view.
     *
     * @param id Id to read.
     * @param nulls if {@code true} - nullable fields should be filled, if {@code false} - otherwise.
     * @return {@code true} if read successfully, {@code false} - otherwise.
     */
    private boolean readKeyValue(int id, boolean nulls) {
        Value res = KV_VIEW.get(Long.valueOf(id));

        if (res == null) {
            return false;
        }

        Value expected = new Value(id, nulls);

        assertEquals(expected, res);

        return true;
    }

    /**
     * Create tuple with specified id and nulls fields filled.
     *
     * @param id Id.
     * @param nulls If {@code true} - nullable fields will be filled.
     * @return Tuple with all requested fields.
     */
    private Tuple createTuple(int id, boolean nulls) {
        Tuple res = Tuple.create();

        for (Column col : SCHEMA.valueColumns().columns()) {
            if (!nulls && col.nullable()) {
                continue;
            }

            String colName = col.name();
            NativeType type = col.type();

            if (NativeTypes.INT8.equals(type)) {
                res.set(colName, (byte) id);
            } else if (NativeTypes.INT16.equals(type)) {
                res.set(colName, (short) id);
            } else if (NativeTypes.INT32.equals(type)) {
                res.set(colName, id);
            } else if (NativeTypes.INT64.equals(type)) {
                res.set(colName, (long) id);
            } else if (NativeTypes.FLOAT.equals(type)) {
                res.set(colName, (float) id);
            } else if (NativeTypes.DOUBLE.equals(type)) {
                res.set(colName, (double) id);
            } else if (NativeTypes.BYTES.equals(type)) {
                res.set(colName, String.valueOf(id).getBytes(StandardCharsets.UTF_8));
            } else if (NativeTypes.STRING.equals(type)) {
                res.set(colName, String.valueOf(id));
            } else if (NativeTypes.UUID.equals(type)) {
                res.set(colName, new UUID(0L, (long) id));
            }

        }

        return res;
    }

    /**
     * Test specified tuple.
     *
     * @param id Expected tuple id.
     * @param t Tuple to test.
     * @param nulls If {@code true} - nullable fields will be filled.
     */
    private void validateTuple(int id, Tuple t, boolean nulls) {
        long actualId = t.longValue("id");

        assertEquals(id, actualId);

        Tuple expected = createTuple(id, nulls);
        expected.set("id", (long) id);

        for (Column col : SCHEMA.valueColumns().columns()) {
            if (!nulls && col.nullable()) {
                continue;
            }

            String colName = col.name();
            NativeType type = col.type();

            if (NativeTypes.INT8.equals(type)) {
                assertEquals(expected.byteValue(colName), t.byteValue(colName));
            } else if (NativeTypes.INT16.equals(type)) {
                assertEquals(expected.shortValue(colName), t.shortValue(colName));
            } else if (NativeTypes.INT32.equals(type)) {
                assertEquals(expected.intValue(colName), t.intValue(colName));
            } else if (NativeTypes.INT64.equals(type)) {
                assertEquals(expected.longValue(colName), t.longValue(colName));
            } else if (NativeTypes.FLOAT.equals(type)) {
                assertEquals(expected.floatValue(colName), t.floatValue(colName));
            } else if (NativeTypes.DOUBLE.equals(type)) {
                assertEquals(expected.doubleValue(colName), t.doubleValue(colName));
            } else if (NativeTypes.BYTES.equals(type)) {
                assertArrayEquals((byte[]) expected.value(colName), (byte[]) t.value(colName));
            } else if (NativeTypes.STRING.equals(type)) {
                assertEquals(expected.stringValue(colName), t.stringValue(colName));
            } else if (NativeTypes.UUID.equals(type)) {
                assertEquals(expected.uuidValue(colName), t.uuidValue(colName));
            }
        }
    }

    /**
     * Class for value in test table.
     */
    private static class Value {
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

        public Value() {
        }

        public Value(int id, boolean nulls) {
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
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Value value = (Value) o;
            return fint8 == value.fint8 && fint16 == value.fint16 && fint32 == value.fint32 && fint64 == value.fint64
                    && Float.compare(value.ffloat, ffloat) == 0 && Double.compare(value.fdouble, fdouble) == 0
                    && Objects.equals(fint8N, value.fint8N) && Objects.equals(fint16N, value.fint16N)
                    && Objects.equals(fint32N, value.fint32N) && Objects.equals(fint64N, value.fint64N)
                    && Objects.equals(ffloatN, value.ffloatN) && Objects.equals(fdoubleN, value.fdoubleN)
                    && Objects.equals(fuuid, value.fuuid) && Objects.equals(fuuidN, value.fuuidN);
        }
    }

    /**
     * Class for row in test table.
     */
    private static class Row {
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

        public Row() {
        }

        public Row(int id, boolean nulls) {
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
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Row row = (Row) o;
            return id == row.id && fint8 == row.fint8 && fint16 == row.fint16 && fint32 == row.fint32 && fint64 == row.fint64
                    && Float.compare(row.ffloat, ffloat) == 0 && Double.compare(row.fdouble, fdouble) == 0
                    && Objects.equals(fint8N, row.fint8N) && Objects.equals(fint16N, row.fint16N) && Objects.equals(
                    fint32N, row.fint32N) && Objects.equals(fint64N, row.fint64N) && Objects.equals(ffloatN, row.ffloatN)
                    && Objects.equals(fdoubleN, row.fdoubleN) && Objects.equals(fuuid, row.fuuid) && Objects.equals(
                    fuuidN, row.fuuidN) && Objects.equals(fstring, row.fstring) && Objects.equals(fstringN, row.fstringN)
                    && Arrays.equals(fbytes, row.fbytes) && Arrays.equals(fbytesN, row.fbytesN);
        }
    }
}
