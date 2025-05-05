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

package org.apache.ignite.internal.sql.engine.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link IgniteTypeFactory}.
 */
public class IgniteTypeFactorySelfTest extends BaseIgniteAbstractTest {

    private static final IgniteTypeFactory TYPE_FACTORY = Commons.typeFactory();

    private final Random random = new Random();

    @BeforeEach
    public void test() {
        long seed = System.nanoTime();
        random.setSeed(seed);
        log.info("Seed: {}", seed);
    }

    @ParameterizedTest
    @MethodSource("types")
    public void leastRestrictiveTypeBetweenTimestampsTypes(List<RelDataType> types, RelDataType expected) {
        RelDataType actual = TYPE_FACTORY.leastRestrictive(types);

        List<RelDataType> shuffledTypes = new ArrayList<>(types);
        Collections.shuffle(shuffledTypes, random);

        assertEquals(expected, actual);
    }

    private static Stream<Arguments> types() {
        return Stream.of(
                // timestamp

                Arguments.of(List.of(
                                timestamp(1)
                        ),
                        timestamp(1)),

                Arguments.of(List.of(
                                timestamp(1),
                                timestamp(3)
                        ),
                        timestamp(3)),

                Arguments.of(List.of(
                                timestamp(1),
                                timestamp(3),
                                timestamp(2)
                        ),
                        timestamp(3)),

                Arguments.of(List.of(
                                timestamp(),
                                timestamp(3)
                        ),
                        timestamp(6)),

                // timestamp ltz

                Arguments.of(List.of(
                                timestampLtz(1)
                        ),
                        timestampLtz(1)),

                Arguments.of(List.of(
                                timestampLtz(1),
                                timestampLtz(3)
                        ),
                        timestampLtz(3)),

                Arguments.of(List.of(
                                timestampLtz(1),
                                timestampLtz(3),
                                timestampLtz(2)
                        ),
                        timestampLtz(3)),

                Arguments.of(List.of(
                                timestampLtz(),
                                timestampLtz(3)
                        ),
                        timestampLtz(6)),

                // timestamp ltz v timestamp

                Arguments.of(List.of(
                                timestamp(1),
                                timestampLtz(3)
                        ),
                        timestamp(3)),

                Arguments.of(List.of(
                                timestampLtz(3),
                                timestamp(1)
                        ),
                        timestamp(3)),

                Arguments.of(List.of(
                                timestampLtz(1),
                                timestampLtz(3),
                                timestamp(2)
                        ),
                        timestamp(3)),

                // other
                Arguments.of(List.of(
                                timestamp(1),
                                timestamp(3),
                                TYPE_FACTORY.createSqlType(SqlTypeName.DATE)
                        ),
                        null),

                Arguments.of(List.of(
                                timestampLtz(1),
                                timestampLtz(3),
                                TYPE_FACTORY.createSqlType(SqlTypeName.DATE)
                        ),
                        null)
        );
    }

    @ParameterizedTest
    @MethodSource("nullableTypes")
    public void leastRestrictiveTypeBetweenTimestampsTypesWithNullability(List<RelDataType> types, RelDataType expected) {
        RelDataType actual = TYPE_FACTORY.leastRestrictive(types);

        List<RelDataType> shuffledTypes = new ArrayList<>(types);
        Collections.shuffle(shuffledTypes, random);

        assertEquals(expected, actual, "Expected: " + typeToString(expected) + " Actual: " + typeToString(actual));
    }

    private static RelDataType timestamp(int p) {
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP, p);
    }

    private static RelDataType timestamp() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
    }

    private static RelDataType timestampLtz(int p) {
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, p);
    }

    private static RelDataType timestampLtz() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    @Nullable
    private static String typeToString(@Nullable RelDataType type) {
        if (type == null) {
            return null;
        } else {
            return type + " nullable: " + type.isNullable();
        }
    }

    private static Stream<Arguments> nullableTypes() {
        return types().flatMap(a -> {
            Object[] args = a.get();
            List<RelDataType> types = (List<RelDataType>) args[0];

            RelDataType expected = (RelDataType) args[1];
            Named<RelDataType> expectedNamed;
            if (expected != null) {
                RelDataType nullableExpectedType = TYPE_FACTORY.createTypeWithNullability(expected, true);
                expectedNamed = Named.of(nullableExpectedType.getFullTypeString(), nullableExpectedType);
            } else {
                expectedNamed = Named.of("null", null);
            }

            List<Arguments> newArgs = new ArrayList<>();

            // [T1, T2] E1
            //  =>
            // [T1 nullable, T2] E1 nullable
            // [T1, T2 nullable] E1 nullable
            for (int i = 0; i < types.size(); i++) {
                List<RelDataType> nullableTypes = new ArrayList<>(types);
                nullableTypes.set(i, TYPE_FACTORY.createTypeWithNullability(nullableTypes.get(i), true));

                String nullableStr = nullableTypes.stream()
                        .map(RelDataType::getFullTypeString)
                        .collect(Collectors.joining());

                Named<List<RelDataType>> typesNamed = Named.of(nullableStr, nullableTypes);
                newArgs.add(Arguments.of(typesNamed, expectedNamed));
            }

            // [T1, T2, NULL] E1 nullable
            List<RelDataType> withNull = new ArrayList<>(types);
            withNull.add(TYPE_FACTORY.createSqlType(SqlTypeName.NULL));
            newArgs.add(Arguments.of(withNull, expectedNamed));

            return newArgs.stream();
        });
    }
}
