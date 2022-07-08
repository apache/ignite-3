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

package org.apache.ignite.internal.schema.serializer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.schema.marshaller.schema.AbstractSchemaSerializer;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.junit.jupiter.api.Test;

/**
 * SchemaDescriptor (de)serializer test.
 */
public class AbstractSerializerTest {
    /**
     * (de)Serialize schema test.
     */
    @Test
    public void schemaSerializeTest() {
        AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        SchemaDescriptor desc = new SchemaDescriptor(7,
                new Column[]{
                        new Column("A", NativeTypes.INT8, false),
                        new Column("B", NativeTypes.INT16, false),
                        new Column("C", NativeTypes.INT32, false),
                        new Column("D", NativeTypes.INT64, false),
                        new Column("E", NativeTypes.UUID, false),
                        new Column("F", NativeTypes.FLOAT, false),
                        new Column("G", NativeTypes.DOUBLE, false),
                        new Column("H", NativeTypes.DATE, false),
                },
                new Column[]{
                        new Column("A1", NativeTypes.stringOf(128), false),
                        new Column("B1", NativeTypes.numberOf(255), false),
                        new Column("C1", NativeTypes.decimalOf(128, 64), false),
                        new Column("D1", NativeTypes.bitmaskOf(256), false),
                        new Column("E1", NativeTypes.datetime(8), false),
                        new Column("F1", NativeTypes.time(8), false),
                        new Column("G1", NativeTypes.timestamp(8), true),
                        new Column("G2", NativeTypes.blobOf(21), false),
                        new Column("G3", NativeTypes.duration(1), false),
                        new Column("G4", NativeTypes.PERIOD, false),
                        new Column("H", NativeTypes.DATE, false, () -> LocalDate.now())
                }
        );

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        assertEquals(desc.version(), deserialize.version());

        assertArrayEquals(desc.keyColumns().columns(), deserialize.keyColumns().columns());
        assertArrayEquals(desc.valueColumns().columns(), deserialize.valueColumns().columns());
        assertArrayEquals(desc.colocationColumns(), deserialize.colocationColumns());
    }

    /**
     * (de)Serialize schema test.
     */
    @Test
    public void columnOrderSerializeTest() {
        AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        Column[] keyCols = {
            new Column(0, "A", NativeTypes.UUID, false),
            new Column(1, "B", NativeTypes.INT64, false),
            new Column(2, "C", NativeTypes.INT8, false),
        };

        Column[] valCols = {
            new Column(3, "A1", NativeTypes.stringOf(128), false),
            new Column(4, "B1", NativeTypes.INT64, false),
        };

        SchemaDescriptor desc = new SchemaDescriptor(7, keyCols, valCols);

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        assertEquals(desc.version(), deserialize.version());

        ArrayList<Column> columns = new ArrayList<>();
        Collections.addAll(columns, keyCols);
        Collections.addAll(columns, valCols);

        for (int i = 0; i < columns.size(); i++) {
            Column col = deserialize.column(i);

            assertEquals(columns.get(col.columnOrder()), col);
        }

        assertArrayEquals(columns.stream().map(Column::name).toArray(String[]::new), desc.columnNames().toArray(String[]::new));
    }

    /**
     * (de)Serialize default value test.
     */
    @Test
    public void defaultValueSerializeTest() {
        AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        var localDateTime = LocalDateTime.now();
        var localTime = LocalTime.now();
        var timeStamp = Instant.now();
        var duration = Duration.ofNanos(7);
        var period = Period.ofMonths(13);
        var date = LocalDate.now();
        var blob = new byte[]{1, 2};

        SchemaDescriptor desc = new SchemaDescriptor(7,
                new Column[]{
                        new Column("A", NativeTypes.INT8, false, () -> (byte) 1),
                        new Column("B", NativeTypes.INT16, false, () -> (short) 1),
                        new Column("C", NativeTypes.INT32, false, () -> 1),
                        new Column("D", NativeTypes.INT64, false, () -> 1L),
                        new Column("E", NativeTypes.UUID, false, () -> new UUID(12, 34)),
                        new Column("F", NativeTypes.FLOAT, false, () -> 1.0f),
                        new Column("G", NativeTypes.DOUBLE, false, () -> 1.0d),
                        new Column("H", NativeTypes.DATE, false, () -> date),
                },
                new Column[]{
                        new Column("A1", NativeTypes.stringOf(128), false, () -> "test"),
                        new Column("B1", NativeTypes.numberOf(255), false, () -> BigInteger.TEN),
                        new Column("C1", NativeTypes.decimalOf(128, 64), false, () -> BigDecimal.TEN),
                        new Column("D1", NativeTypes.bitmaskOf(256), false, BitSet::new),
                        new Column("E1", NativeTypes.datetime(8), false, () -> localDateTime),
                        new Column("F1", NativeTypes.time(8), false, () -> localTime),
                        new Column("G1", NativeTypes.timestamp(8), true, () -> timeStamp),
                        new Column("G2", NativeTypes.blobOf(21), false, () -> blob),
                        new Column("G3", NativeTypes.duration(1), false, () -> duration),
                        new Column("G4", NativeTypes.PERIOD, false, () -> period)
                }
        );

        byte[] serialized = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialized);

        //key columns
        assertEquals(deserialize.column("A").defaultValue(), (byte) 1);
        assertEquals(deserialize.column("B").defaultValue(), (short) 1);
        assertEquals(deserialize.column("C").defaultValue(), 1);
        assertEquals(deserialize.column("D").defaultValue(), 1L);
        assertEquals(deserialize.column("E").defaultValue(), new UUID(12, 34));
        assertEquals(deserialize.column("F").defaultValue(), 1.0f);
        assertEquals(deserialize.column("G").defaultValue(), 1.0d);
        assertEquals(deserialize.column("H").defaultValue(), date);

        //value columns
        assertEquals(deserialize.column("A1").defaultValue(), "test");
        assertEquals(deserialize.column("B1").defaultValue(), BigInteger.TEN);
        assertEquals(deserialize.column("C1").defaultValue(), BigDecimal.TEN);
        assertEquals(deserialize.column("D1").defaultValue(), new BitSet());
        assertEquals(deserialize.column("E1").defaultValue(), localDateTime);
        assertEquals(deserialize.column("F1").defaultValue(), localTime);
        assertEquals(deserialize.column("G1").defaultValue(), timeStamp);
        assertArrayEquals((byte[]) deserialize.column("G2").defaultValue(), blob);
        assertEquals(deserialize.column("G3").defaultValue(), duration);
        assertEquals(deserialize.column("G4").defaultValue(), period);
    }

    /**
     * (de)Serialize column mapping test.
     */
    @Test
    public void columnMappingSerializeTest() {
        final AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        SchemaDescriptor desc = new SchemaDescriptor(100500,
                new Column[]{
                        new Column("A", NativeTypes.INT8, false, () -> (byte) 1)
                },
                new Column[]{
                        new Column("A1", NativeTypes.stringOf(128), false, () -> "test"),
                        new Column("B1", NativeTypes.numberOf(255), false, () -> BigInteger.TEN)
                }
        );

        ColumnMapper mapper = ColumnMapping.createMapper(desc);

        mapper.add(0, 1);

        Column c1 = new Column("C1", NativeTypes.stringOf(128), false, () -> "brandNewColumn").copy(2);

        mapper.add(c1);

        desc.columnMapping(mapper);

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        ColumnMapper mapper1 = deserialize.columnMapping();

        assertEquals(1, mapper1.map(0));
        assertEquals(c1, mapper1.mappedColumn(2));
    }
}
