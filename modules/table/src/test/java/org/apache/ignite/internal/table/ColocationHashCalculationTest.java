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

import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.RowTest;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.util.HashCalculator;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Check calculation hash by colocation columns specified at the schema.
 */
public class ColocationHashCalculationTest {
    /** Random. */
    private Random rnd;

    /**
     * Initialization.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        IgniteLogger.forClass(RowTest.class).info("Using seed: " + seed + "L; //");

        rnd = new Random(seed);
    }

    @Test
    public void simple() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{
                        new Column(0, "id0", INT8, false),
                        new Column(1, "id1", INT32, false),
                        new Column(2, "id2", STRING, false),
                },
                new Column[]{new Column(3, "val", INT32, true).copy(3)});

        RowAssembler rasm = new RowAssembler(schema, 1, 0);

        rasm.appendByte((byte) 1);
        rasm.appendInt(2);
        rasm.appendString("key_" + 3);
        rasm.appendInt(0);

        Row r = new Row(schema, rasm.build());

        HashCalculator hashCalc = new HashCalculator();
        hashCalc.appendByte((byte) 1);
        hashCalc.appendInt(2);
        hashCalc.appendString("key_" + 3);

        assertEquals(hashCalc.hash(), colocationHash(r));
    }

    @Test
    public void allTypes() throws TupleMarshallerException {
        Column[] keyCols = IntStream.range(0, SchemaTestUtils.ALL_TYPES.size())
                .mapToObj(i -> {
                    NativeType t = SchemaTestUtils.ALL_TYPES.get(i);
                    return new Column(i, "id_" + t.spec().name(), t, false);
                })
                .toArray(Column[]::new);

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols,
                new Column[]{new Column("val", INT32, true).copy(keyCols.length)});

        Row r = generateRandomRow(rnd, schema);
        assertEquals(colocationHash(r), r.colocationHash());

        TupleMarshaller marshaller = new TupleMarshallerImpl(new DummySchemaManagerImpl(schema));
        for (int i = 0; i < 10; ++i) {
            Column rndCol = schema.column(rnd.nextInt(schema.length()));

            Tuple t = TableRow.tuple(r);
            t.set(rndCol.name(), SchemaTestUtils.generateRandomValue(rnd, rndCol.type()));

            r = new Row(schema, new ByteBufferRow(marshaller.marshal(t).bytes()));

            assertEquals(colocationHash(r), r.colocationHash());
        }
    }

    private static Row generateRandomRow(Random rnd, @NotNull SchemaDescriptor schema) throws TupleMarshallerException {
        TupleMarshaller marshaller = new TupleMarshallerImpl(new DummySchemaManagerImpl(schema));

        Tuple t = Tuple.create();

        for (int i = 0; i < schema.length(); ++ i) {
            Column c = schema.column(i);

            t.set(c.name(), SchemaTestUtils.generateRandomValue(rnd, c.type()));
        }

        return new Row(schema, new ByteBufferRow(marshaller.marshal(t).bytes()));
    }

    private int colocationHash(Row r) {
        HashCalculator hashCalc = new HashCalculator();
        for (Column c : r.schema().colocationColumns()) {
            hashCalc.append(r.value(c.schemaIndex()));
        }

        return hashCalc.hash();
    }
}
