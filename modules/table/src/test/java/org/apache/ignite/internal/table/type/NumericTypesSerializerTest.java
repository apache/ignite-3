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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.TupleBuilderImpl;
import org.apache.ignite.internal.table.TupleMarshallerImpl;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** */
public class NumericTypesSerializerTest {
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
    public void testNumber(Pair<BigInteger, BigInteger> pair) {
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
    @Test
    public void testPrecisionRestrictionsForNumbers() {
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
        assertThrows(InvalidTypeException.class, () -> badTup.set("number1", BigInteger.valueOf(999991L)), "Column's type mismatch");
        assertThrows(InvalidTypeException.class, () -> badTup.set("number1", new BigInteger("111111")), "Column's type mismatch");
        assertThrows(InvalidTypeException.class, () -> badTup.set("number1", BigInteger.valueOf(-999991L)), "Column's type mismatch");
        assertThrows(InvalidTypeException.class, () -> badTup.set("number1", new BigInteger("-111111")), "Column's type mismatch");
    }

    /**
     *
     */
    @Test
    public void testPrecisionRestrictionsForDecimal() {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {
                new Column("decimalCol", NativeTypes.decimalOf(12, 3), false),
            }
        );

        final TupleBuilderImpl badTup = new TupleBuilderImpl(schema);

        badTup.set("key", rnd.nextLong());
        assertThrows(InvalidTypeException.class, () -> badTup.set("decimalCol", new BigDecimal("123456789.0123")), "Column's type mismatch");
        assertThrows(InvalidTypeException.class, () -> badTup.set("decimalCol", new BigDecimal("-1234567890123")), "Column's type mismatch");
        assertThrows(InvalidTypeException.class, () -> badTup.set("decimalCol", new BigDecimal(123456789.0123d)), "Column's type mismatch");
    }

    /**
     *
     */
    @Test
    public void testScaleRestrictionsForDecimal() {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {
                new Column("decimalCol1", NativeTypes.decimalOf(22, 0), false),
                new Column("decimalCol2", NativeTypes.decimalOf(22, 0), false),
                new Column("decimalCol3", NativeTypes.decimalOf(22, 0), false),
                new Column("decimalCol4", NativeTypes.decimalOf(22, 0), false),
                new Column("decimalCol5", NativeTypes.from(ColumnType.decimalOf()), false),
            }
        );

        final TupleBuilderImpl tup = new TupleBuilderImpl(schema);

        tup.set("key", rnd.nextLong());
        tup.set("decimalCol1", new BigDecimal("123456789.0123"));
        tup.set("decimalCol2", new BigDecimal("123456789.1"));
        tup.set("decimalCol3", new BigDecimal("123456789.112312315413"));
        tup.set("decimalCol4", new BigDecimal("0E+132"));
        tup.set("decimalCol5", new BigDecimal("123456789.0123"));

        final Row row = marshaller.marshal(new TupleBuilderImpl(schema).set("key", rnd.nextLong()).build(), tup.build());

        assertEquals(row.decimalValue(1), new BigDecimal("123456789"));
        assertEquals(row.decimalValue(2), new BigDecimal("123456789"));
        assertEquals(row.decimalValue(3), new BigDecimal("123456789"));
        assertEquals(row.decimalValue(4), new BigDecimal("0"));
        assertEquals(row.decimalValue(5), new BigDecimal("123456789.012"));
    }
}
