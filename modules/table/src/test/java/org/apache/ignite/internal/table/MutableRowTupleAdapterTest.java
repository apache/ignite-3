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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.DATE;
import static org.apache.ignite.internal.type.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.type.NativeTypes.FLOAT;
import static org.apache.ignite.internal.type.NativeTypes.INT16;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.INT8;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.apache.ignite.internal.type.NativeTypes.datetime;
import static org.apache.ignite.internal.type.NativeTypes.time;
import static org.apache.ignite.internal.type.NativeTypes.timestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.AbstractMutableTupleTest;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Tests server tuple builder implementation.
 *
 * <p>The class contains implementation-specific tests. Tuple interface contract conformance/violation tests are inherited from the base
 * class.
 */
public class MutableRowTupleAdapterTest extends AbstractMutableTupleTest {
    /** Schema descriptor for default test tuple. */
    private final SchemaDescriptor schema = new SchemaDescriptor(
            42,
            List.of(
                    new Column("ID", INT64, false),
                    new Column("SIMPLENAME", STRING, true),
                    new Column("QuotedName", STRING, true),
                    new Column("NOVALUE", STRING, true)
            ),
            List.of("ID"),
            null
    );

    /** Schema descriptor for tuple with columns of all the supported types. */
    private final SchemaDescriptor fullSchema = new SchemaDescriptor(42,
            List.of(
                    new Column("valBoolCol".toUpperCase(), BOOLEAN, true),
                    new Column("valByteCol".toUpperCase(), INT8, true),
                    new Column("valShortCol".toUpperCase(), INT16, true),
                    new Column("valIntCol".toUpperCase(), INT32, true),
                    new Column("valLongCol".toUpperCase(), INT64, true),
                    new Column("valFloatCol".toUpperCase(), FLOAT, true),
                    new Column("valDoubleCol".toUpperCase(), DOUBLE, true),
                    new Column("valDateCol".toUpperCase(), DATE, true),
                    new Column("keyUuidCol".toUpperCase(), NativeTypes.UUID, false),
                    new Column("valUuidCol".toUpperCase(), NativeTypes.UUID, false),
                    new Column("valTimeCol".toUpperCase(), time(TIME_PRECISION), true),
                    new Column("valDateTimeCol".toUpperCase(), datetime(TIMESTAMP_PRECISION), true),
                    new Column("valTimeStampCol".toUpperCase(), timestamp(TIMESTAMP_PRECISION), true),
                    new Column("valBytesCol".toUpperCase(), BYTES, false),
                    new Column("valStringCol".toUpperCase(), STRING, false),
                    new Column("valDecimalCol".toUpperCase(), NativeTypes.decimalOf(25, 5), false)
            ),
            List.of("keyUuidCol".toUpperCase()),
            null
    );

    @Test
    public void testKeyValueChunks() {
        SchemaDescriptor schema = new SchemaDescriptor(
                42,
                List.of(
                        new Column("ID2", INT64, false),
                        new Column("NAME", STRING, true),
                        new Column("PRICE", DOUBLE, true),
                        new Column("ID1", INT64, false)
                ),
                List.of("ID1", "ID2"),
                null
        );

        Tuple original = Tuple.create()
                .set("id1", 3L)
                .set("name", "Shirt")
                .set("price", 5.99d)
                .set("id2", 33L);

        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schema);

        Row row = marshaller.marshal(original);

        Tuple key = TableRow.keyTuple(row);
        Tuple val = TableRow.valueTuple(row);

        assertEquals(3L, (Long) key.value("id1"));
        assertEquals(3L, (Long) key.value(0));

        assertEquals(33L, (Long) key.value("id2"));
        assertEquals(33L, (Long) key.value(1));

        assertEquals("Shirt", val.value("name"));
        assertEquals("Shirt", val.value(0));

        assertEquals(5.99d, val.value("price"));
        assertEquals(5.99d, val.value(1));

        // Wrong columns.
        assertThrows(IndexOutOfBoundsException.class, () -> key.value(2));
        assertThrows(IllegalArgumentException.class, () -> key.value("price"));

        assertThrows(IndexOutOfBoundsException.class, () -> val.value(2));
        assertThrows(IllegalArgumentException.class, () -> val.value("id"));
    }

    @Test
    public void testRowTupleMutability() {
        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schema);

        Row row = marshaller.marshal(Tuple.create().set("id", 1L).set("simpleName", "Shirt"));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        final Tuple val = TableRow.valueTuple(row);

        tuple.set("id", 2L);

        assertEquals(2L, (Long) tuple.value("id"));
        assertEquals(1L, (Long) key.value("id"));

        tuple.set("simpleName", "noname");

        assertEquals("noname", tuple.value("simpleName"));
        assertEquals("Shirt", val.value("simpleName"));

        tuple.set("foo", "bar");

        assertEquals("bar", tuple.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> key.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> val.value("foo"));
    }

    @Test
    public void testKeyValueTupleMutability() {
        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schema);

        Row row = marshaller.marshal(Tuple.create().set("id", 1L).set("simpleName", "Shirt"));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        final Tuple val = TableRow.valueTuple(row);

        assertTrue(tuple instanceof SchemaAware);

        key.set("id", 3L);

        assertEquals(3L, (Long) key.value("id"));
        assertEquals(1L, (Long) tuple.value("id"));

        val.set("simpleName", "noname");

        assertEquals("noname", val.value("simpleName"));
        assertEquals("Shirt", tuple.value("simpleName"));

        val.set("foo", "bar");

        assertEquals("bar", val.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> key.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> tuple.value("foo"));
    }

    @Test
    public void testRowTupleSchemaAwareness() {
        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schema);

        Row row = marshaller.marshal(Tuple.create().set("id", 1L).set("simpleName", "Shirt"));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        final Tuple val = TableRow.valueTuple(row);

        assertTrue(tuple instanceof SchemaAware);

        assertNotNull(((SchemaAware) tuple).schema());
        assertNotNull(((SchemaAware) key).schema());
        assertNotNull(((SchemaAware) val).schema());

        tuple.set("name", "noname");

        assertNull(((SchemaAware) tuple).schema());
        assertNotNull(((SchemaAware) key).schema());
        assertNotNull(((SchemaAware) val).schema());
    }

    @Test
    public void testKeyValueTupleSchemaAwareness() {
        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schema);

        Row row = marshaller.marshal(Tuple.create().set("id", 1L).set("simpleName", "Shirt"));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        Tuple val = TableRow.valueTuple(row);

        assertTrue(tuple instanceof SchemaAware);

        assertNotNull(((SchemaAware) tuple).schema());
        assertNotNull(((SchemaAware) key).schema());
        assertNotNull(((SchemaAware) val).schema());

        key.set("simpleName", "bar");

        assertNotNull(((SchemaAware) tuple).schema());
        assertNull(((SchemaAware) key).schema());
        assertNotNull(((SchemaAware) val).schema());

        val.set("id", 1L);

        assertNotNull(((SchemaAware) tuple).schema());
        assertNull(((SchemaAware) key).schema());
        assertNull(((SchemaAware) val).schema());
    }

    @Test
    public void testTupleEqualityCompatibility() {
        var tuple = getTupleWithColumnOfAllTypes();
        var referenceTuple = Tuple.create();

        for (int i = 0; i < tuple.columnCount(); i++) {
            referenceTuple.set(tuple.columnName(i), tuple.value(i));
        }

        assertEquals(tuple, referenceTuple);
        assertEquals(referenceTuple, tuple);
        assertEquals(tuple.hashCode(), referenceTuple.hashCode());
    }

    @Test
    public void testExtendedTupleEquality() throws Exception {
        Tuple keyTuple = Tuple.create().set("keyUuidCol", UUID_VALUE);
        Tuple valTuple = addColumnOfAllTypes(Tuple.create());
        Tuple tuple = Tuple.copy(valTuple).set(keyTuple.columnName(0), keyTuple.value(0));

        // Check tuples backed with Row.
        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(fullSchema);

        Row row = marshaller.marshal(keyTuple, valTuple);

        Tuple rowKeyTuple = TableRow.keyTuple(row);
        Tuple rowValTuple = TableRow.valueTuple(row);
        Tuple rowTuple = TableRow.tuple(marshaller.marshal(tuple));

        assertEquals(keyTuple.columnCount(), rowKeyTuple.columnCount());
        assertEquals(valTuple.columnCount(), rowValTuple.columnCount());
        assertEquals(tuple.columnCount(), tuple.columnCount());

        assertEquals(keyTuple, rowKeyTuple);
        assertEquals(rowKeyTuple, keyTuple);

        assertEquals(valTuple, rowValTuple);
        assertEquals(rowValTuple, valTuple);

        assertEquals(tuple, rowTuple);
        assertEquals(rowTuple, tuple);

        // Check deserialized.
        Tuple keyTuple2 = deserializeTuple(serializeTuple(rowKeyTuple));
        Tuple valTuple2 = deserializeTuple(serializeTuple(rowValTuple));
        final Tuple tuple2 = deserializeTuple(serializeTuple(rowTuple));

        assertEquals(keyTuple, keyTuple2);
        assertEquals(keyTuple2, keyTuple);

        assertEquals(valTuple, valTuple2);
        assertEquals(valTuple2, valTuple);

        assertEquals(tuple, tuple2);
        assertEquals(tuple2, tuple);

        // Check the tuples backed with Row after update.
        rowKeyTuple.set("foo", "bar");
        rowValTuple.set("foo", "bar");
        rowTuple.set("foo", "bar");

        assertNotEquals(keyTuple, rowKeyTuple);
        assertNotEquals(rowKeyTuple, keyTuple);

        assertNotEquals(valTuple, rowValTuple);
        assertNotEquals(rowValTuple, valTuple);

        assertNotEquals(tuple, rowTuple);
        assertNotEquals(rowTuple, tuple);

        // Update original to make them equal.
        keyTuple.set("foo", "bar");
        valTuple.set("foo", "bar");
        tuple.set("foo", "bar");

        assertEquals(keyTuple, rowKeyTuple);
        assertEquals(rowKeyTuple, keyTuple);

        assertEquals(valTuple, rowValTuple);
        assertEquals(rowValTuple, valTuple);

        assertEquals(tuple, rowTuple);
        assertEquals(rowTuple, tuple);

    }

    @Test
    public void testKeyValueSerialization() throws Exception {
        Tuple key1 = Tuple.create().set("keyUuidCol", UUID.randomUUID());
        Tuple val1 = addColumnOfAllTypes(Tuple.create());

        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(fullSchema);

        Row row = marshaller.marshal(key1, val1);

        Tuple key2 = deserializeTuple(serializeTuple(TableRow.keyTuple(row)));
        Tuple val2 = deserializeTuple(serializeTuple(TableRow.valueTuple(row)));

        assertEquals(key1, key2);
        assertEquals(val1, val2);
    }

    @Test
    public void testTupleNetworkSerialization() throws Exception {
        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schema);

        Row row = marshaller.marshal(Tuple.create().set("id", 1L).set("simpleName", "Shirt"));

        Tuple tuple = TableRow.tuple(row);

        var userObjectDescriptorRegistry = new ClassDescriptorRegistry();
        var userObjectDescriptorFactory = new ClassDescriptorFactory(userObjectDescriptorRegistry);

        var userObjectMarshaller = new DefaultUserObjectMarshaller(userObjectDescriptorRegistry, userObjectDescriptorFactory);

        Tuple unmarshalled = userObjectMarshaller.unmarshal(userObjectMarshaller.marshal(tuple).bytes(), userObjectDescriptorRegistry);

        assertEquals(tuple, unmarshalled);
    }

    @Test
    void testTemporalValuesPrecisionConstraint() throws Exception {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(1,
                new Column[]{new Column("KEY", INT32, false)},
                new Column[]{
                        new Column("TIME", time(2), true),
                        new Column("DATETIME", datetime(2), true),
                        new Column("TIMESTAMP", timestamp(2), true)
                }
        );

        Tuple tuple = Tuple.create().set("key", 1)
                .set("time", LocalTime.of(1, 2, 3, 456_700_000))
                .set("datetime", LocalDateTime.of(2022, 1, 2, 3, 4, 5, 678_900_000))
                .set("timestamp", Instant.ofEpochSecond(123, 456_700_000));
        Tuple expected = Tuple.create().set("key", 1)
                .set("time", LocalTime.of(1, 2, 3, 450_000_000))
                .set("datetime", LocalDateTime.of(2022, 1, 2, 3, 4, 5, 670_000_000))
                .set("timestamp", Instant.ofEpochSecond(123, 450_000_000));

        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schemaDescriptor);

        Row row = marshaller.marshal(tuple);

        Tuple tuple1 = deserializeTuple(serializeTuple(TableRow.tuple(row)));

        assertEquals(expected, tuple1);
    }

    @Test
    void testVarlenValuesLengthConstraints() throws Exception {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(1,
                new Column[]{new Column("KEY", INT32, false)},
                new Column[]{
                        new Column("STRING", NativeTypes.stringOf(5), true),
                        new Column("BYTES", NativeTypes.blobOf(5), true),
                }
        );

        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schemaDescriptor);

        Tuple tuple1 = Tuple.create().set("key", 1)
                .set("string", "abcef")
                .set("bytes", new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        Tuple tuple2 = Tuple.create().set("key", 1)
                .set("string", "abcefghi")
                .set("bytes", new byte[]{1, 2, 3, 4, 5});

        assertThrowsWithCause(() -> marshaller.marshal(tuple1), InvalidTypeException.class,
                "Value too long [column='BYTES', type=BYTE_ARRAY(5)]"
        );
        assertThrowsWithCause(() -> marshaller.marshal(tuple2), InvalidTypeException.class,
                "Value too long [column='STRING', type=STRING(5)]"
        );

        Tuple expected = Tuple.create().set("key", 1)
                .set("string", "abc")
                .set("bytes", new byte[]{1, 2, 3});

        Row row = marshaller.marshal(expected);

        assertEquals(expected, deserializeTuple(serializeTuple(TableRow.tuple(row))));
    }

    @Test
    void testDecimalPrecisionConstraint() {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(1,
                new Column[]{new Column("KEY", INT32, false)},
                new Column[]{
                        new Column("DECIMAL", NativeTypes.decimalOf(7, 2), true),
                }
        );

        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schemaDescriptor);

        Tuple tuple1 = Tuple.create().set("key", 1).set("decimal", new BigDecimal("123456.7"));

        assertThrowsWithCause(() -> marshaller.marshal(tuple1), SchemaMismatchException.class,
                "Numeric field overflow in column 'DECIMAL'");
    }

    @Test
    void testDecimalScaleConstraint() throws Exception {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(1,
                new Column[]{new Column("KEY", INT32, false)},
                new Column[]{
                        new Column("DECIMAL", NativeTypes.decimalOf(5, 2), true),
                }
        );

        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schemaDescriptor);

        Tuple tuple = Tuple.create().set("key", 1).set("decimal", new BigDecimal("123.458"));
        Tuple expected = Tuple.create().set("key", 1).set("decimal", new BigDecimal("123.46")); // Rounded.

        Row row = marshaller.marshal(tuple);

        assertEquals(expected, deserializeTuple(serializeTuple(TableRow.tuple(row))));
    }

    @Test
    public void testFullSchemaHasAllTypes() {
        Set<ColumnType> schemaTypes = fullSchema.columns().stream()
                .map(Column::type)
                .map(NativeType::spec)
                .collect(Collectors.toSet());

        for (ColumnType columnType : NativeType.nativeTypes()) {
            assertTrue(schemaTypes.contains(columnType), "Schema does not contain " + columnType);
        }
    }

    @Override
    protected Tuple createTuple(Function<Tuple, Tuple> transformer) {
        Tuple tuple = Tuple.create().set("ID", 1L);

        tuple = transformer.apply(tuple);

        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schema);

        return TableRow.tuple(marshaller.marshal(tuple));
    }

    @Override
    protected Tuple getTuple() {
        Tuple tuple = Tuple.create();

        tuple = addColumnsForDefaultSchema(tuple);

        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schema);

        return TableRow.tuple(marshaller.marshal(tuple));
    }

    @Override
    protected Tuple getTupleWithColumnOfAllTypes() {
        Tuple tuple = Tuple.create().set("keyUuidCol", UUID_VALUE);

        tuple = addColumnOfAllTypes(tuple);

        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(fullSchema);

        return TableRow.tuple(marshaller.marshal(tuple));
    }

    @Override
    protected Tuple createTupleOfSingleColumn(ColumnType type, String columnName, Object value) {
        NativeType nativeType = NativeTypes.fromObject(value);
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column(columnName.toUpperCase(), nativeType, false)},
                new Column[]{}
        );

        Tuple tuple = Tuple.create().set(columnName, value);

        TupleMarshaller marshaller = KeyValueTestUtils.createMarshaller(schema);

        return TableRow.tuple(marshaller.marshal(tuple));
    }
}
