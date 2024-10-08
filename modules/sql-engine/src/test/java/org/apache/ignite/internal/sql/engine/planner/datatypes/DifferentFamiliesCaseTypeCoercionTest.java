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

package org.apache.ignite.internal.sql.engine.planner.datatypes;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.DifferentFamiliesPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A set of tests to verify behavior of type coercion for CASE operator, when operands belongs to the different type families.
 */
@SuppressWarnings("ThrowableNotThrown")
public class DifferentFamiliesCaseTypeCoercionTest extends BaseTypeCoercionTest {
    private static final IgniteSchema SCHEMA = createSchemaWithTwoColumnTable(NativeTypes.STRING, NativeTypes.STRING);

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void caseWithTableColumns(TypePair typePair) {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN c1 ELSE c2 END FROM t",
                        schema, anything -> true),
                "Illegal mixing of types in CASE or COALESCE statement"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN c2 ELSE c1 END FROM t",
                        schema, anything -> true),
                "Illegal mixing of types in CASE or COALESCE statement"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT CASE c1 WHEN c2 THEN 'foo' ELSE 'bar' END FROM t",
                        schema, anything -> true),
                "Cannot apply '=' to arguments of type"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT CASE c2 WHEN c1 THEN 'foo' ELSE 'bar' END FROM t",
                        schema, anything -> true),
                "Cannot apply '=' to arguments of type"
        );
    }

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void caseWithDynamicParams(TypePair typePair) {
        Object firstVal = SqlTestUtils.generateValueByType(typePair.first());
        Object secondVal = SqlTestUtils.generateValueByType(typePair.second());

        assert firstVal != null;
        assert secondVal != null;

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN ? ELSE ? END FROM t",
                        SCHEMA, anything -> true, List.of(firstVal, secondVal)),
                "Illegal mixing of types in CASE or COALESCE statement"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN ? ELSE ? END FROM t",
                        SCHEMA, anything -> true, List.of(secondVal, firstVal)),
                "Illegal mixing of types in CASE or COALESCE statement"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT CASE ? WHEN ? THEN 'foo' ELSE 'bar' END FROM t",
                        SCHEMA, anything -> true, List.of(firstVal, secondVal)),
                "Cannot apply '=' to arguments of type"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT CASE ? WHEN ? THEN 'foo' ELSE 'bar' END FROM t",
                        SCHEMA, anything -> true, List.of(secondVal, firstVal)),
                "Cannot apply '=' to arguments of type"
        );
    }

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void caseWithLiteral(TypePair typePair) {
        Assumptions.assumeTrue(
                typePair.first() != NativeTypes.UUID && typePair.second() != NativeTypes.UUID,
                "Literal of type UUID is not supported"
        );

        String firstLiteral = generateLiteralWithNoRepetition(typePair.first());
        String secondLiteral = generateLiteralWithNoRepetition(typePair.second());

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan(format("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN {} ELSE {} END FROM t",
                        firstLiteral, secondLiteral), SCHEMA, anything -> true),
                "Illegal mixing of types in CASE or COALESCE statement"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan(format("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN {} ELSE {} END FROM t",
                        secondLiteral, firstLiteral), SCHEMA, anything -> true),
                "Illegal mixing of types in CASE or COALESCE statement"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan(format("SELECT CASE {} WHEN {} THEN 'foo' ELSE 'bar' END FROM t",
                        firstLiteral, secondLiteral), SCHEMA, anything -> true),
                "Cannot apply '=' to arguments of type"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan(format("SELECT CASE {} WHEN {} THEN 'foo' ELSE 'bar' END FROM t",
                        secondLiteral, firstLiteral), SCHEMA, anything -> true),
                "Cannot apply '=' to arguments of type"
        );
    }
}
