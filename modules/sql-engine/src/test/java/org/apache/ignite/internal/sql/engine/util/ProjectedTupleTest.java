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

package org.apache.ignite.internal.sql.engine.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests to verify projected tuple facade. */
class ProjectedTupleTest {
    private static final IgniteLogger LOG = Loggers.forClass(ProjectedTupleTest.class);

    private static final BinaryTupleSchema ALL_TYPES_SCHEMA = BinaryTupleSchema.create(new Element[]{
            new Element(NativeTypes.BOOLEAN, true),
            new Element(NativeTypes.INT8, true),
            new Element(NativeTypes.INT16, true),
            new Element(NativeTypes.INT32, true),
            new Element(NativeTypes.INT64, true),
            new Element(NativeTypes.FLOAT, true),
            new Element(NativeTypes.DOUBLE, true),
            new Element(NativeTypes.decimalOf(10, 2), true),
            new Element(NativeTypes.UUID, true),
            new Element(NativeTypes.stringOf(10), true),
            new Element(NativeTypes.blobOf(10), true),
            new Element(NativeTypes.bitmaskOf(10), true),
            new Element(NativeTypes.numberOf(10), true),
            new Element(NativeTypes.DATE, true),
            new Element(NativeTypes.time(), true),
            new Element(NativeTypes.datetime(), true),
            new Element(NativeTypes.timestamp(), true)
    });

    private static final Map<NativeTypeSpec, Object> VALUES = new EnumMap<>(NativeTypeSpec.class);

    private static final BinaryTuple TUPLE;

    static {
        VALUES.put(NativeTypeSpec.BOOLEAN, true);
        VALUES.put(NativeTypeSpec.INT8, (byte) 1);
        VALUES.put(NativeTypeSpec.INT16, (short) 1);
        VALUES.put(NativeTypeSpec.INT32, 1);
        VALUES.put(NativeTypeSpec.INT64, 1L);
        VALUES.put(NativeTypeSpec.FLOAT, 1.0f);
        VALUES.put(NativeTypeSpec.DOUBLE, 1.0);
        VALUES.put(NativeTypeSpec.DECIMAL, BigDecimal.ONE);
        VALUES.put(NativeTypeSpec.UUID, new UUID(0L, 1L));
        VALUES.put(NativeTypeSpec.STRING, "1");
        VALUES.put(NativeTypeSpec.BYTES, new byte[]{1});
        VALUES.put(NativeTypeSpec.BITMASK, new BitSet());
        VALUES.put(NativeTypeSpec.NUMBER, BigInteger.ONE);
        VALUES.put(NativeTypeSpec.DATE, LocalDate.now());
        VALUES.put(NativeTypeSpec.TIME, LocalTime.now());
        VALUES.put(NativeTypeSpec.DATETIME, LocalDateTime.now());
        VALUES.put(NativeTypeSpec.TIMESTAMP, Instant.now());

        var builder = new BinaryTupleBuilder(ALL_TYPES_SCHEMA.elementCount());

        for (int i = 0; i < ALL_TYPES_SCHEMA.elementCount(); i++) {
            Element e = ALL_TYPES_SCHEMA.element(i);

            BinaryRowConverter.appendValue(builder, e, VALUES.get(e.typeSpec()));
        }

        TUPLE = new BinaryTuple(ALL_TYPES_SCHEMA.elementCount(), builder.build());
    }

    private static Random RND;

    @BeforeAll
    static void initRandom() {
        int seed = ThreadLocalRandom.current().nextInt();

        RND = new Random(seed);

        LOG.info("Seed is " + seed);
    }

    @Test
    void allTypesAreCovered() {
        List<NativeTypeSpec> coveredTypes = IntStream.range(0, ALL_TYPES_SCHEMA.elementCount())
                .mapToObj(ALL_TYPES_SCHEMA::element)
                .map(Element::typeSpec)
                .collect(Collectors.toList());

        EnumSet<NativeTypeSpec> allTypes = EnumSet.allOf(NativeTypeSpec.class);

        coveredTypes.forEach(allTypes::remove);

        assertThat(allTypes, empty());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void projectionReturnsProperElementCount(int projectionSize) {
        InternalTuple projection1 = new ProjectedTuple(
                ALL_TYPES_SCHEMA, TUPLE, new int[projectionSize]
        );

        assertThat(projection1.elementCount(), equalTo(projectionSize));
    }

    @Test
    void testProjection() {
        int f1 = RND.nextInt(ALL_TYPES_SCHEMA.elementCount());
        int f2 = RND.nextInt(ALL_TYPES_SCHEMA.elementCount());
        int f3 = RND.nextInt(ALL_TYPES_SCHEMA.elementCount());

        int[] projection = {f1, f2, f3};

        InternalTuple projectedTuple = new ProjectedTuple(
                ALL_TYPES_SCHEMA, TUPLE, projection
        );

        Element e1 = ALL_TYPES_SCHEMA.element(f1);
        Element e2 = ALL_TYPES_SCHEMA.element(f2);
        Element e3 = ALL_TYPES_SCHEMA.element(f3);

        BinaryTupleSchema projectedSchema = BinaryTupleSchema.create(new Element[] {
                e1, e2, e3
        });

        assertThat(projectedSchema.value(projectedTuple, 0), equalTo(ALL_TYPES_SCHEMA.value(TUPLE, f1)));
        assertThat(projectedSchema.value(projectedTuple, 1), equalTo(ALL_TYPES_SCHEMA.value(TUPLE, f2)));
        assertThat(projectedSchema.value(projectedTuple, 2), equalTo(ALL_TYPES_SCHEMA.value(TUPLE, f3)));

        InternalTuple restored = new BinaryTuple(projection.length, projectedTuple.byteBuffer());

        assertThat(projectedSchema.value(restored, 0), equalTo(ALL_TYPES_SCHEMA.value(TUPLE, f1)));
        assertThat(projectedSchema.value(restored, 1), equalTo(ALL_TYPES_SCHEMA.value(TUPLE, f2)));
        assertThat(projectedSchema.value(restored, 2), equalTo(ALL_TYPES_SCHEMA.value(TUPLE, f3)));
    }
}