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

import static org.apache.ignite.internal.schema.SchemaTestUtils.specToType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests to verify projected tuple facade. */
class ProjectedTupleTest {
    private static final IgniteLogger LOG = Loggers.forClass(ProjectedTupleTest.class);

    private static final BinaryTupleSchema ALL_TYPES_SCHEMA = BinaryTupleSchema.create(
            SchemaTestUtils.ALL_TYPES.stream()
                    .map(type -> new Element(type, true))
                    .toArray(Element[]::new)
    );

    private static BinaryTuple TUPLE;
    private static Random RND;

    @BeforeAll
    static void initRandom() {
        int seed = ThreadLocalRandom.current().nextInt();

        RND = new Random(seed);

        LOG.info("Seed is " + seed);

        var builder = new BinaryTupleBuilder(ALL_TYPES_SCHEMA.elementCount());

        for (int i = 0; i < ALL_TYPES_SCHEMA.elementCount(); i++) {
            Element e = ALL_TYPES_SCHEMA.element(i);

            BinaryRowConverter.appendValue(builder, e, SchemaTestUtils.generateRandomValue(RND, specToType(e.typeSpec())));
        }

        TUPLE = new BinaryTuple(ALL_TYPES_SCHEMA.elementCount(), builder.build());
    }

    @Test
    void allTypesAreCovered() {
        List<NativeTypeSpec> coveredTypes = IntStream.range(0, ALL_TYPES_SCHEMA.elementCount())
                .mapToObj(ALL_TYPES_SCHEMA::element)
                .map(Element::typeSpec)
                .collect(Collectors.toList());

        EnumSet<NativeTypeSpec> allTypes = EnumSet.allOf(NativeTypeSpec.class);

        Set<NativeTypeSpec> unsupported = Set.of(NativeTypeSpec.BITMASK, NativeTypeSpec.NUMBER);
        allTypes.removeAll(unsupported);

        coveredTypes.forEach(allTypes::remove);

        assertThat(allTypes, empty());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void projectionReturnsProperElementCount(int projectionSize) {
        InternalTuple projection1 = new FieldDeserializingProjectedTuple(
                ALL_TYPES_SCHEMA, TUPLE, new int[projectionSize]
        );
        InternalTuple projection2 = new FormatAwareProjectedTuple(
                TUPLE, new int[projectionSize]
        );

        assertThat(projection1.elementCount(), equalTo(projectionSize));
        assertThat(projection2.elementCount(), equalTo(projectionSize));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testProjection(boolean useOptimizeProjection) {
        int f1 = RND.nextInt(ALL_TYPES_SCHEMA.elementCount());
        int f2 = RND.nextInt(ALL_TYPES_SCHEMA.elementCount());
        int f3 = RND.nextInt(ALL_TYPES_SCHEMA.elementCount());

        int[] projection = {f1, f2, f3};

        InternalTuple projectedTuple = useOptimizeProjection
                ? new FormatAwareProjectedTuple(TUPLE, projection)
                : new FieldDeserializingProjectedTuple(ALL_TYPES_SCHEMA, TUPLE, projection);

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
