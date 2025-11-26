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
 * A set of tests to verify behavior of type coercion for INSERT operations, when values belongs to the different type families.
 */
@SuppressWarnings("ThrowableNotThrown")
public class DifferentFamiliesInsertSourcesCoercionTest extends BaseTypeCoercionTest {

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void insertLiterals(TypePair typePair) {
        Assumptions.assumeTrue(
                typePair.first() != NativeTypes.UUID && typePair.second() != NativeTypes.UUID,
                "Literal of type UUID is not supported"
        );

        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        // SHORT values can intersect with a DECIMAL with a 5 digits in integer parts, so for SHORT (INT16) we need to generate values
        // take it into consideration.

        String firstLiteral = generateLiteral(typePair.first(), typePair.second().spec() == ColumnType.INT16);

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("INSERT INTO t VALUES(" + firstLiteral + "," + firstLiteral + ")", schema, anything -> true),
                "Cannot assign to target field 'C2' of type"
        );

        String secondLiteral = generateLiteral(typePair.second(), typePair.first().spec() == ColumnType.INT16);

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("INSERT INTO t VALUES(" + secondLiteral + "," + secondLiteral + ")", schema, anything -> true),
                "Cannot assign to target field 'C1' of type"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("INSERT INTO t VALUES(" + firstLiteral + "," + secondLiteral + ")," 
                        + " (" + firstLiteral + "," + firstLiteral + ")", schema, anything -> true),
                "Values passed to VALUES operator must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("INSERT INTO t VALUES(" + firstLiteral + "," + secondLiteral + "),"
                        + " (" + secondLiteral + "," + secondLiteral + ")", schema, anything -> true),
                "Values passed to VALUES operator must have compatible types"
        );
    }

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void insertDynamicParameters(TypePair typePair) {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        Object firstVal = SqlTestUtils.generateValueByType(typePair.first());
        Object secondVal = SqlTestUtils.generateValueByType(typePair.second());

        assert firstVal != null;
        assert secondVal != null;

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("INSERT INTO t VALUES(?, ?)", schema, anything -> true, List.of(firstVal, firstVal)),
                "Cannot assign to target field 'C2' of type"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("INSERT INTO t VALUES(?, ?)", schema, anything -> true, List.of(secondVal, secondVal)),
                "Cannot assign to target field 'C1' of type"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("INSERT INTO t VALUES(?, ?), (?, ?)", schema, anything -> true,
                        List.of(firstVal, secondVal, firstVal, firstVal)),
                "Values passed to VALUES operator must have compatible types"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("INSERT INTO t VALUES(?, ?), (?, ?)", schema, anything -> true,
                        List.of(firstVal, secondVal, secondVal, secondVal)),
                "Values passed to VALUES operator must have compatible types"
        );
    }

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void insertFromSourceTable(TypePair typePair) {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("INSERT INTO t SELECT c1, c1 FROM t", schema, anything -> true),
                "Cannot assign to target field 'C2' of type"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("INSERT INTO t SELECT c2, c2 FROM t", schema, anything -> true),
                "Cannot assign to target field 'C1' of type"
        );
    }
}
