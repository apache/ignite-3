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

import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT8;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.util.HashCalculator;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

        Loggers.forClass(ColocationHashCalculationTest.class).info("Using seed: " + seed + "L; //");

        rnd = new Random(seed);
    }

    @Test
    public void simple() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{
                        new Column("ID0", INT8, false),
                        new Column("ID1", INT32, false),
                        new Column("ID2", STRING, false),
                },
                new Column[]{new Column("VAL", INT32, true)});

        RowAssembler rasm = new RowAssembler(schema, -1);

        rasm.appendByte((byte) 1);
        rasm.appendInt(2);
        rasm.appendString("key_" + 3);
        rasm.appendInt(0);

        Row r = Row.wrapBinaryRow(schema, rasm.build());

        HashCalculator hashCalc = new HashCalculator();
        hashCalc.appendByte((byte) 1);
        hashCalc.appendInt(2);
        hashCalc.appendString("key_" + 3);

        int[] hashes = new int[3];
        hashes[0] = HashCalculator.hashByte((byte) 1);
        hashes[1] = HashCalculator.hashInt(2);
        hashes[2] = HashCalculator.hashString("key_" + 3);

        assertEquals(hashCalc.hash(), colocationHash(r));
        assertEquals(hashCalc.hash(), HashCalculator.combinedHash(hashes));
    }

    @ParameterizedTest
    @MethodSource("allTypesArgs")
    public void partialCombination(NativeType type) {
        Object[] vals = {
                SchemaTestUtils.generateRandomValue(rnd, type),
                SchemaTestUtils.generateRandomValue(rnd, type),
                SchemaTestUtils.generateRandomValue(rnd, type),
        };

        var calculator = new HashCalculator();

        for (Object val : vals) {
            calculator.append(val, scaleOrElse(type, -1), precisionOrElse(type, -1));
        }

        int expected = calculator.hash();

        for (int i = 0; i < vals.length; i++) {
            calculator.reset();

            for (int j = 0; j < vals.length; j++) {
                if (i == j) {
                    calculator.combine(HashCalculator.hashValue(vals[j], scaleOrElse(type, -1), precisionOrElse(type, -1)));
                } else {
                    calculator.append(vals[j], scaleOrElse(type, -1), precisionOrElse(type, -1));
                }
            }

            assertEquals(expected, calculator.hash());
        }
    }

    private static Stream<Arguments> allTypesArgs() {
        return SchemaTestUtils.ALL_TYPES.stream().map(Arguments::of);
    }

    @Test
    public void allTypes() {
        Column[] keyCols = IntStream.range(0, SchemaTestUtils.ALL_TYPES.size())
                .mapToObj(i -> {
                    NativeType t = SchemaTestUtils.ALL_TYPES.get(i);
                    return new Column("ID_" + t.spec().name().toUpperCase(), t, false);
                })
                .toArray(Column[]::new);

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols,
                new Column[]{new Column("VAL", INT32, true)});

        Row r = generateRandomRow(rnd, schema);
        assertEquals(colocationHash(r), r.colocationHash());

        TupleMarshaller marshaller = new TupleMarshallerImpl(schema);
        for (int i = 0; i < 10; ++i) {
            Column rndCol = schema.column(rnd.nextInt(schema.length()));

            Tuple t = TableRow.tuple(r);
            t.set(rndCol.name(), SchemaTestUtils.generateRandomValue(rnd, rndCol.type()));

            r = marshaller.marshal(t);

            assertEquals(colocationHash(r), r.colocationHash());
        }
    }

    @Test
    void collisions() {
        var set = new HashSet<Integer>();
        int collisions = 0;

        for (var key1 = 0; key1 < 100; key1++) {
            for (var key2 = 0; key2 < 100; key2++) {
                for (var key3 = 0; key3 < 100; key3++) {
                    HashCalculator hashCalc = new HashCalculator();
                    hashCalc.appendInt(key1);
                    hashCalc.appendInt(key2);
                    hashCalc.appendInt(key3);

                    int hash = hashCalc.hash();
                    if (set.contains(hash)) {
                        collisions++;
                    } else {
                        set.add(hash);
                    }
                }
            }
        }

        assertEquals(125, collisions);
    }

    @Test
    void distribution() {
        int partitions = 100;
        var map = new HashMap<Integer, Integer>();

        for (var key1 = 0; key1 < 100; key1++) {
            for (var key2 = 0; key2 < 100; key2++) {
                for (var key3 = 0; key3 < 100; key3++) {
                    HashCalculator hashCalc = new HashCalculator();
                    hashCalc.appendInt(key1);
                    hashCalc.appendInt(key2);
                    hashCalc.appendInt(key3);

                    int hash = hashCalc.hash();
                    int partition = Math.abs(hash % partitions);

                    map.put(partition, map.getOrDefault(partition, 0) + 1);
                }
            }
        }

        var maxSkew = 326;

        for (var entry : map.entrySet()) {
            // CSV to plot: System.out.println(entry.getKey() + ", " + entry.getValue());
            assertTrue(entry.getValue() < 10_000 + maxSkew, "Partition " + entry.getKey() + " keys: " + entry.getValue());
            assertTrue(entry.getValue() > 10_000 - maxSkew, "Partition " + entry.getKey() + " keys: " + entry.getValue());
        }
    }

    private static Row generateRandomRow(Random rnd, SchemaDescriptor schema) {
        TupleMarshaller marshaller = new TupleMarshallerImpl(schema);

        Tuple t = Tuple.create();

        for (int i = 0; i < schema.length(); ++ i) {
            Column c = schema.column(i);

            t.set(c.name(), SchemaTestUtils.generateRandomValue(rnd, c.type()));
        }

        return marshaller.marshal(t);
    }

    private static int colocationHash(Row r) {
        HashCalculator hashCalc = new HashCalculator();
        for (Column c : r.schema().colocationColumns()) {
            var scale = c.type() instanceof DecimalNativeType ? ((DecimalNativeType) c.type()).scale() : 0;
            var precision = c.type() instanceof TemporalNativeType ? ((TemporalNativeType) c.type()).precision() : 0;
            hashCalc.append(r.value(c.positionInRow()), scale, precision);
        }

        return hashCalc.hash();
    }

    private static int precisionOrElse(NativeType type, int another) {
        if (type instanceof TemporalNativeType) {
            return ((TemporalNativeType) type).precision();
        }

        if (type instanceof DecimalNativeType) {
            return ((DecimalNativeType) type).precision();
        }

        return another;
    }

    private static int scaleOrElse(NativeType type, int another) {
        if (type instanceof DecimalNativeType) {
            return ((DecimalNativeType) type).scale();
        }

        return another;
    }
}
