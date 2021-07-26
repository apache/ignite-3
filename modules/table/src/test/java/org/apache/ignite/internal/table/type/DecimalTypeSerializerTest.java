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

package org.apache.ignite.internal.table.type;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.TupleBuilderImpl;
import org.apache.ignite.internal.table.TupleMarshallerImpl;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** */
public class DecimalTypeSerializerTest {
    /** Random. */
    private static Random rnd = new Random();


    /** Tuple marshaller. */
    private TupleMarshaller marshaller;

    /** Schema descriptor. */
    private SchemaDescriptor schema;

    /**
     * @return List of BigInteger pairs for test.
     */
    private static List<Pair<BigInteger, BigInteger>> numbers() {
        return Arrays.asList(
            new Pair<>(BigInteger.valueOf(10L), BigInteger.valueOf(10)),
            new Pair<>(BigInteger.valueOf(-10L), BigInteger.valueOf(-10)),
            new Pair<>(new BigInteger("10"), BigInteger.valueOf(10)),
            new Pair<>(new BigInteger("1000").divide(BigInteger.TEN), BigInteger.valueOf(10).multiply(BigInteger.TEN)),
            new Pair<>(new BigInteger("999999999"), BigInteger.valueOf(999999999L)),
            new Pair<>(new BigInteger("+999999999"), BigInteger.valueOf(999999999L)),
            new Pair<>(new BigInteger("-999999999"), BigInteger.valueOf(-999999999L))
        );
    }

    @BeforeEach
    public void setup() {
        long seed = System.currentTimeMillis();

        rnd = new Random(seed);

        marshaller = new TupleMarshallerImpl(new SchemaRegistryImpl(v -> null) {
            @Override public SchemaDescriptor schema() {
                return schema;
            }

            @Override public SchemaDescriptor schema(int ver) {
                return schema;
            }

            @Override public int lastSchemaVersion() {
                return schema.version();
            }
        });

    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("numbers")
    public void testFixLenNumber(Pair<BigInteger, BigInteger> pair) {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {
                new Column("number1", NativeTypes.numberOf(19), false),
                new Column("number2", NativeTypes.numberOf(10), false)
            }
        );

        final TupleBuilderImpl tup = new TupleBuilderImpl(schema);

        tup.set("key", rnd.nextLong());
        tup.set("number1", pair.getFirst());
        tup.set("number2", pair.getSecond());

        Tuple keyTuple = new TupleBuilderImpl(schema).set("key", rnd.nextLong()).build();

        final Row row = marshaller.marshal(keyTuple, tup.build());

        assertEquals(row.numberValue(1), row.numberValue(2));
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("numbers")
    public void testVarLenNumbers(Pair<BigInteger, BigInteger> pair) {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {
                new Column("number1", NativeTypes.VL_NUMBER, false),
                new Column("number2", NativeTypes.VL_NUMBER, false)
            }
        );

        final TupleBuilderImpl tup = new TupleBuilderImpl(schema);

        tup.set("key", rnd.nextLong());
        tup.set("number1", pair.getFirst());
        tup.set("number2", pair.getSecond());

        Tuple keyTuple = new TupleBuilderImpl(schema).set("key", rnd.nextLong()).build();

        final Row row = marshaller.marshal(keyTuple, tup.build());

        assertEquals(row.varLenNumberValue(1), row.varLenNumberValue(2));
    }

    /**
     *
     */
    @Test
    public void testPrecisionRestrictionsForFixLenNumbers() {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {
                new Column("number1", NativeTypes.numberOf(5), false),
            }
        );

        final TupleBuilderImpl badTup = new TupleBuilderImpl(schema);

        badTup.set("key", rnd.nextLong());
        badTup.set("number1", BigInteger.valueOf(999991L));

        Tuple keyTuple = new TupleBuilderImpl(schema).set("key", rnd.nextLong()).build();

        assertThrows(IllegalArgumentException.class, () -> marshaller.marshal(keyTuple, badTup.build()), "Failed to set number value for column");

        final TupleBuilderImpl anotherBadTup = new TupleBuilderImpl(schema);

        badTup.set("key", rnd.nextLong());
        badTup.set("number1", new BigInteger("111111"));

        assertThrows(IllegalArgumentException.class, () -> marshaller.marshal(keyTuple, anotherBadTup.build()), "Failed to set number value for column");

    }
}
