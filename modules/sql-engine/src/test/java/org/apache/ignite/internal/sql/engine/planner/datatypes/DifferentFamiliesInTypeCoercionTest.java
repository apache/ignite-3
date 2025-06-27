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
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A set of tests to verify behavior of type coercion for IN operator, when operands belongs to the different type families.
 */
@SuppressWarnings("ThrowableNotThrown")
public class DifferentFamiliesInTypeCoercionTest extends BaseTypeCoercionTest {
    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void columns(TypePair typePair) {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t WHERE c1 IN (c2)", schema, anything -> true),
                "Values passed to IN operator must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t WHERE c2 IN (c1)", schema, anything -> true),
                "Values passed to IN operator must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t WHERE c1 IN (c1, c2)", schema, anything -> true),
                "Values in expression list must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t WHERE c2 IN (c1, c2)", schema, anything -> true),
                "Values in expression list must have compatible types"
        );
    }

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void dynamicParamsOnLeft(TypePair typePair) {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        Object firstVal = SqlTestUtils.generateValueByType(typePair.first());
        Object secondVal = SqlTestUtils.generateValueByType(typePair.second());

        assert firstVal != null;
        assert secondVal != null;

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t WHERE ? IN (c2)", schema, anything -> true, List.of(firstVal)),
                "Values passed to IN operator must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t WHERE ? IN (c1)", schema, anything -> true, List.of(secondVal)),
                "Values passed to IN operator must have compatible types"
        );
    }

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void dynamicParamsOnRight(TypePair typePair) {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        Object firstVal = SqlTestUtils.generateValueByType(typePair.first());
        Object secondVal = SqlTestUtils.generateValueByType(typePair.second());

        assert firstVal != null;
        assert secondVal != null;

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t WHERE c1 IN (?)", schema, anything -> true, List.of(secondVal)),
                "Values passed to IN operator must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t WHERE c2 IN (?)", schema, anything -> true, List.of(firstVal)),
                "Values passed to IN operator must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t WHERE c1 IN (?, ?)", schema, anything -> true, List.of(firstVal, secondVal)),
                "Values in expression list must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t WHERE c2 IN (?, ?)", schema, anything -> true, List.of(firstVal, secondVal)),
                "Values in expression list must have compatible types"
        );
    }

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void literalsOnLeft(TypePair typePair) {
        Assumptions.assumeTrue(
                typePair.first() != NativeTypes.UUID && typePair.second() != NativeTypes.UUID,
                "Literal of type UUID is not supported"
        );

        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        String firstLiteral = generateLiteral(typePair.first(), typePair.second().spec() == ColumnType.INT16);
        String secondLiteral = generateLiteral(typePair.second(), typePair.first().spec() == ColumnType.INT16);

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan(format(
                        "SELECT c1 FROM t WHERE {} IN (c2)", firstLiteral
                ), schema, anything -> true),
                "Values passed to IN operator must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan(format(
                        "SELECT c1 FROM t WHERE {} IN (c1)", secondLiteral
                ), schema, anything -> true),
                "Values passed to IN operator must have compatible types"
        );
    }

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void literalsOnRight(TypePair typePair) {
        Assumptions.assumeTrue(
                typePair.first() != NativeTypes.UUID && typePair.second() != NativeTypes.UUID,
                "Literal of type UUID is not supported"
        );

        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        String firstLiteral = generateLiteral(typePair.first(), typePair.second().spec() == ColumnType.INT16);
        String secondLiteral = generateLiteral(typePair.second(), typePair.first().spec() == ColumnType.INT16);

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan(format(
                        "SELECT c1 FROM t WHERE c1 IN ({})", secondLiteral
                ), schema, anything -> true),
                "Values passed to IN operator must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan(format(
                        "SELECT c1 FROM t WHERE c2 IN ({})", firstLiteral
                ), schema, anything -> true),
                "Values passed to IN operator must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan(format(
                        "SELECT c1 FROM t WHERE c1 IN ({}, {})", firstLiteral, secondLiteral
                ), schema, anything -> true),
                "Values in expression list must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan(format(
                        "SELECT c1 FROM t WHERE c2 IN ({}, {})", firstLiteral, secondLiteral
                ), schema, anything -> true),
                "Values in expression list must have compatible types"
        );
    }
}
