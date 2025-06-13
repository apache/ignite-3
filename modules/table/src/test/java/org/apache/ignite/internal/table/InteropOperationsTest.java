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

package org.apache.ignite.internal.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.table.distributed.schema.ConstantSchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for different access methods:
 * 1) Create single table.
 * 2) Write using different API's into it (row 1 - with all values, row 2 - with nulls).
 * 3) Read data back through all possible APIs and validate it.
 */
@ExtendWith(ConfigurationExtension.class)
public class InteropOperationsTest extends BaseIgniteAbstractTest {
    /** Test schema. */
    private static SchemaDescriptor schema;

    /** Table for tests. */
    private static TableViewInternal table;

    /** Dummy internal table for tests. */
    private static DummyInternalTableImpl intTable;

    /** Key value binary view for test. */
    private static KeyValueView<Tuple, Tuple> kvBinView;

    /** Key value view for test. */
    private static KeyValueView<Long, Value> kvView;

    /** Record view for test. */
    private static RecordView<Row> rView;

    /** Record binary view for test. */
    private static RecordView<Tuple> rBinView;

    @InjectConfiguration
    private static TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private static SystemDistributedConfiguration systemDistributedConfiguration;

    @InjectConfiguration
    private static ReplicationConfiguration replicationConfiguration;

    @BeforeAll
    static void beforeAll() {
        NativeType[] types = {
                NativeTypes.BOOLEAN,
                NativeTypes.INT8, NativeTypes.INT16, NativeTypes.INT32, NativeTypes.INT64,
                NativeTypes.FLOAT, NativeTypes.DOUBLE, NativeTypes.UUID, NativeTypes.STRING,
                NativeTypes.BYTES, NativeTypes.DATE, NativeTypes.time(0), NativeTypes.timestamp(4), NativeTypes.datetime(4),
                NativeTypes.decimalOf(5, 2),
        };

        List<Column> valueCols = new ArrayList<>(types.length * 2);

        for (NativeType type : types) {
            String colName = "F" + type.spec().name().toUpperCase();

            valueCols.add(new Column(colName, type, false));
            valueCols.add(new Column(colName + "N", type, true));
        }

        int schemaVersion = 1;

        schema = new SchemaDescriptor(schemaVersion,
                new Column[]{new Column("ID", NativeTypes.INT64, false)},
                valueCols.toArray(Column[]::new)
        );

        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.topologyService().localMember().address()).thenReturn(DummyInternalTableImpl.ADDR);

        intTable = new DummyInternalTableImpl(mock(ReplicaService.class, RETURNS_DEEP_STUBS), schema, txConfiguration,
                systemDistributedConfiguration,
                replicationConfiguration);

        SchemaRegistry schemaRegistry = new DummySchemaManagerImpl(schema);

        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class, RETURNS_DEEP_STUBS));

        SchemaVersions schemaVersions = new ConstantSchemaVersions(schemaVersion);

        table = new TableImpl(intTable, schemaRegistry, lockManager(), schemaVersions, mock(IgniteSql.class), -1);

        kvBinView = table.keyValueView();
        kvView =  table.keyValueView(Mapper.of(Long.class, "id"), Mapper.of(Value.class));
        rBinView = table.recordView();
        rView = table.recordView(Mapper.of(Row.class));
    }

    private static LockManager lockManager() {
        HeapLockManager lockManager = HeapLockManager.smallInstance();
        lockManager.start(new WaitDieDeadlockPreventionPolicy());
        return lockManager;
    }

    /**
     * Validate all types are tested.
     */
    @Test
    public void ensureAllTypesTested() {
        SchemaTestUtils.ensureAllTypesChecked(schema.valueColumns().stream());
    }

    @AfterEach
    public void clearTable() {
        table.recordView().delete(null, Tuple.create().set("id", 1L));
        table.recordView().delete(null, Tuple.create().set("id", 2L));
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
        assertFalse(readKeyValue(3, false));

        assertTrue(readKeyValueBinary(1, false));
        assertTrue(readKeyValueBinary(2, true));
        assertFalse(readKeyValueBinary(3, false));

        assertTrue(readRecord(1, false));
        assertTrue(readRecord(2, true));
        assertFalse(readRecord(3, false));

        assertTrue(readRecordBinary(1, false));
        assertTrue(readRecordBinary(2, true));
        assertFalse(readRecordBinary(3, false));
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

        kvBinView.put(null, k, v);
    }

    /**
     * Read through binary view.
     *
     * @param id Id to read.
     * @return {@code true} if read successfully, {@code false} - otherwise.
     */
    private boolean readKeyValueBinary(int id, boolean nulls) {
        Tuple k = Tuple.create().set("id", (long) id);

        Tuple v = kvBinView.get(null, k);
        boolean contains = kvBinView.contains(null, k);

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

        assertTrue(rView.insert(null, r1));
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

        Row actual = rView.get(null, expected);

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

        assertTrue(rBinView.insert(null, t1));
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

        Tuple res = rBinView.get(null, k);

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
        kvView.put(null, (long) id, new Value(id, nulls));
    }

    /**
     * Read through binary view.
     *
     * @param id Id to read.
     * @param nulls if {@code true} - nullable fields should be filled, if {@code false} - otherwise.
     * @return {@code true} if read successfully, {@code false} - otherwise.
     */
    private boolean readKeyValue(int id, boolean nulls) {
        Value res = kvView.get(null, Long.valueOf(id));

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

        for (Column col : schema.valueColumns()) {
            if (!nulls && col.nullable()) {
                continue;
            }

            String colName = col.name();
            NativeType type = col.type();

            switch (type.spec()) {
                case INT8:
                    res.set(colName, (byte) id);
                    break;
                case INT16:
                    res.set(colName, (short) id);
                    break;
                case INT32:
                    res.set(colName, id);
                    break;
                case INT64:
                    res.set(colName, (long) id);
                    break;
                case FLOAT:
                    res.set(colName, (float) id);
                    break;
                case DOUBLE:
                    res.set(colName, (double) id);
                    break;
                case DECIMAL:
                    res.set(colName, BigDecimal.valueOf(id * 100).movePointLeft(2));
                    break;
                case UUID:
                    res.set(colName, new UUID(0L, id));
                    break;
                case STRING:
                    res.set(colName, String.valueOf(id));
                    break;
                case BYTE_ARRAY:
                    res.set(colName, String.valueOf(id).getBytes(StandardCharsets.UTF_8));
                    break;
                case DATE:
                    res.set(colName, LocalDate.ofYearDay(2021, id));
                    break;
                case TIME:
                    res.set(colName, LocalTime.ofSecondOfDay(id));
                    break;
                case DATETIME:
                    res.set(colName, LocalDateTime.ofEpochSecond(id, 0, ZoneOffset.UTC));
                    break;
                case TIMESTAMP:
                    res.set(colName, Instant.ofEpochSecond(id));
                    break;
                case BOOLEAN:
                    res.set(colName, id % 2 == 0);
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected type: " + type);
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

        for (Column col : schema.valueColumns()) {
            if (!nulls && col.nullable()) {
                continue;
            }

            String colName = col.name();
            NativeType type = col.type();
            ColumnType typeSpec = type.spec();

            switch (typeSpec) {
                case BOOLEAN:
                    assertEquals(expected.booleanValue(colName), t.booleanValue(colName));
                    break;
                case INT8:
                    assertEquals(expected.byteValue(colName), t.byteValue(colName));
                    break;
                case INT16:
                    assertEquals(expected.shortValue(colName), t.shortValue(colName));
                    break;
                case INT32:
                    assertEquals(expected.intValue(colName), t.intValue(colName));
                    break;
                case INT64:
                    assertEquals(expected.longValue(colName), t.longValue(colName));
                    break;
                case FLOAT:
                    assertEquals(expected.floatValue(colName), t.floatValue(colName));
                    break;
                case DOUBLE:
                    assertEquals(expected.doubleValue(colName), t.doubleValue(colName));
                    break;
                case BYTE_ARRAY:
                    assertArrayEquals(expected.value(colName), (byte[]) t.value(colName));
                    break;
                case STRING:
                    assertEquals(expected.stringValue(colName), t.stringValue(colName));
                    break;
                case UUID:
                    assertEquals(expected.uuidValue(colName), t.uuidValue(colName));
                    break;
                case DATE:
                    assertEquals(expected.dateValue(colName), t.dateValue(colName));
                    break;
                case TIME:
                    assertEquals(expected.timeValue(colName), t.timeValue(colName));
                    break;
                case DATETIME:
                    assertEquals(expected.datetimeValue(colName), t.datetimeValue(colName));
                    break;
                case TIMESTAMP:
                    assertEquals(expected.timestampValue(colName), t.timestampValue(colName));
                    break;
                case DECIMAL:
                    assertEquals(expected.decimalValue(colName), t.decimalValue(colName));
                    break;
                default:
                    throw new IllegalArgumentException("Expected type: " + type);
            }
        }
    }

    /**
     * Class for value in test table.
     */
    private static class Value {
        private boolean fboolean;
        private Boolean fbooleanN;
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
        //CHECKSTYLE:OFF
        private byte[] fbyte_array;
        private byte[] fbyte_arrayN;
        //CHECKSTYLE:ON
        private LocalDate fdate;
        private LocalDate fdateN;
        private LocalTime ftime;
        private LocalTime ftimeN;
        private LocalDateTime fdatetime;
        private LocalDateTime fdatetimeN;
        private Instant ftimestamp;
        private Instant ftimestampN;
        private BigDecimal fdecimal;
        private BigDecimal fdecimalN;

        public Value() {

        }

        public Value(int id, boolean nulls) {
            fboolean = id % 2 == 0;
            fbooleanN = (nulls) ? id % 2 == 0 : null;
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
            fbyte_array = String.valueOf(id).getBytes(StandardCharsets.UTF_8);
            fbyte_arrayN = (nulls) ? String.valueOf(id).getBytes(StandardCharsets.UTF_8) : null;
            fdate = LocalDate.ofYearDay(2021, id);
            fdateN = (nulls) ? LocalDate.ofYearDay(2021, id) : null;
            ftime = LocalTime.ofSecondOfDay(id);
            ftimeN = (nulls) ? LocalTime.ofSecondOfDay(id) : null;
            fdatetime = LocalDateTime.ofEpochSecond(id, 0, ZoneOffset.UTC);
            fdatetimeN = (nulls) ? LocalDateTime.ofEpochSecond(id, 0, ZoneOffset.UTC) : null;
            ftimestamp = Instant.ofEpochSecond(id);
            ftimestampN = (nulls) ? Instant.ofEpochSecond(id) : null;
            fdecimal = BigDecimal.valueOf(id * 100).movePointLeft(2);
            fdecimalN = (nulls) ? BigDecimal.valueOf(id * 100).movePointLeft(2) : null;
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
            return fboolean == value.fboolean && Objects.equals(fbooleanN, value.fbooleanN)
                    && fint8 == value.fint8 && fint16 == value.fint16 && fint32 == value.fint32 && fint64 == value.fint64
                    && Float.compare(value.ffloat, ffloat) == 0 && Double.compare(value.fdouble, fdouble) == 0
                    && Objects.equals(fint8N, value.fint8N) && Objects.equals(fint16N, value.fint16N)
                    && Objects.equals(fint32N, value.fint32N) && Objects.equals(fint64N, value.fint64N)
                    && Objects.equals(ffloatN, value.ffloatN) && Objects.equals(fdoubleN, value.fdoubleN)
                    && Objects.equals(fuuid, value.fuuid) && Objects.equals(fuuidN, value.fuuidN) && Objects.equals(
                    fstring, value.fstring) && Objects.equals(fstringN, value.fstringN) && Arrays.equals(fbyte_array, value.fbyte_array)
                    && Arrays.equals(fbyte_arrayN, value.fbyte_arrayN) && Objects.equals(fdate, value.fdate) && Objects.equals(
                    fdateN, value.fdateN) && Objects.equals(ftime, value.ftime) && Objects.equals(ftimeN, value.ftimeN)
                    && Objects.equals(fdatetime, value.fdatetime) && Objects.equals(fdatetimeN, value.fdatetimeN)
                    && Objects.equals(ftimestamp, value.ftimestamp) && Objects.equals(ftimestampN, value.ftimestampN)
                    && Objects.equals(fdecimal, value.fdecimal) && Objects.equals(fdecimalN, value.fdecimalN);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    fboolean,
                    fbooleanN,
                    fint8,
                    fint8N,
                    fint16,
                    fint16N,
                    fint32,
                    fint32N,
                    fint64,
                    fint64N,
                    ffloat,
                    ffloatN,
                    fdouble,
                    fdoubleN,
                    fuuid,
                    fuuidN,
                    fstring,
                    fstringN,
                    fbyte_array,
                    fbyte_arrayN,
                    fdate,
                    fdateN,
                    ftime,
                    ftimeN,
                    fdatetime,
                    fdatetimeN,
                    ftimestamp,
                    ftimestampN,
                    fdecimal,
                    fdecimalN
            );
        }
    }

    /**
     * Class for row in test table.
     */
    private static class Row {
        private long id;
        private boolean fboolean;
        private Boolean fbooleanN;
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
        //CHECKSTYLE:OFF
        private byte[] fbyte_array;
        private byte[] fbyte_arrayN;
        //CHECKSTYLE:ON
        private LocalDate fdate;
        private LocalDate fdateN;
        private LocalTime ftime;
        private LocalTime ftimeN;
        private LocalDateTime fdatetime;
        private LocalDateTime fdatetimeN;
        private Instant ftimestamp;
        private Instant ftimestampN;
        private BigDecimal fdecimal;
        private BigDecimal fdecimalN;

        public Row() {
        }

        public Row(int id, boolean nulls) {
            this.id = id;
            fboolean = id % 2 == 0;
            fbooleanN = (nulls) ? id % 2 == 0 : null;
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

            fbyte_array = String.valueOf(id).getBytes(StandardCharsets.UTF_8);
            fbyte_arrayN = (nulls) ? String.valueOf(id).getBytes(StandardCharsets.UTF_8) : null;

            fdate = LocalDate.ofYearDay(2021, id);
            fdateN = (nulls) ? LocalDate.ofYearDay(2021, id) : null;
            ftime = LocalTime.ofSecondOfDay(id);
            ftimeN = (nulls) ? LocalTime.ofSecondOfDay(id) : null;
            fdatetime = LocalDateTime.ofEpochSecond(id, 0, ZoneOffset.UTC);
            fdatetimeN = (nulls) ? LocalDateTime.ofEpochSecond(id, 0, ZoneOffset.UTC) : null;
            ftimestamp = Instant.ofEpochSecond(id);
            ftimestampN = (nulls) ? Instant.ofEpochSecond(id) : null;
            fdecimal = BigDecimal.valueOf(id * 100).movePointLeft(2);
            fdecimalN = (nulls) ? BigDecimal.valueOf(id * 100).movePointLeft(2) : null;
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
            return id == row.id && fboolean == row.fboolean && Objects.equals(fbooleanN, row.fbooleanN)
                    && fint8 == row.fint8 && fint16 == row.fint16 && fint32 == row.fint32 && fint64 == row.fint64
                    && Float.compare(row.ffloat, ffloat) == 0 && Double.compare(row.fdouble, fdouble) == 0
                    && Objects.equals(fint8N, row.fint8N) && Objects.equals(fint16N, row.fint16N) && Objects.equals(
                    fint32N, row.fint32N) && Objects.equals(fint64N, row.fint64N) && Objects.equals(ffloatN, row.ffloatN)
                    && Objects.equals(fdoubleN, row.fdoubleN) && Objects.equals(fuuid, row.fuuid) && Objects.equals(
                    fuuidN, row.fuuidN) && Objects.equals(fstring, row.fstring) && Objects.equals(fstringN, row.fstringN)
                    && Arrays.equals(fbyte_array, row.fbyte_array) && Arrays.equals(fbyte_arrayN, row.fbyte_arrayN) && Objects.equals(
                    fdate, row.fdate) && Objects.equals(fdateN, row.fdateN) && Objects.equals(ftime, row.ftime)
                    && Objects.equals(ftimeN, row.ftimeN) && Objects.equals(fdatetime, row.fdatetime)
                    && Objects.equals(fdatetimeN, row.fdatetimeN) && Objects.equals(ftimestamp, row.ftimestamp)
                    && Objects.equals(ftimestampN, row.ftimestampN) && Objects.equals(fdecimal, row.fdecimal)
                    && Objects.equals(fdecimalN, row.fdecimalN);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    id,
                    fboolean,
                    fbooleanN,
                    fint8,
                    fint8N,
                    fint16,
                    fint16N,
                    fint32,
                    fint32N,
                    fint64,
                    fint64N,
                    ffloat,
                    ffloatN,
                    fdouble,
                    fdoubleN,
                    fuuid,
                    fuuidN,
                    fstring,
                    fstringN,
                    fbyte_array,
                    fbyte_arrayN,
                    fdate,
                    fdateN,
                    ftime,
                    ftimeN,
                    fdatetime,
                    fdatetimeN,
                    ftimestamp,
                    ftimestampN,
                    fdecimal,
                    fdecimalN
            );
        }
    }
}
