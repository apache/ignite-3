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

import org.apache.calcite.runtime.CalciteContextException;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.DifferentFamiliesPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A set of tests to verify behavior of type coercion for Set operations (UNION, INTERSECT and EXCEPT), when values belongs to different
 * type families.
 */
@SuppressWarnings("ThrowableNotThrown")
public class DifferentFamiliesSetOperatorCoercionTest extends BaseTypeCoercionTest {
    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void unionOperator(TypePair typePair) {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t UNION ALL SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of UNION ALL"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t UNION ALL SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of UNION ALL"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t UNION SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of UNION"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t UNION SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of UNION"
        );
    }

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void exceptOperator(TypePair typePair) {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t EXCEPT ALL SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of EXCEPT ALL"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t EXCEPT ALL SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of EXCEPT ALL"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t EXCEPT SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of EXCEPT"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t EXCEPT SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of EXCEPT"
        );
    }

    @ParameterizedTest
    @EnumSource(DifferentFamiliesPair.class)
    public void intersectOperator(TypePair typePair) {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t INTERSECT ALL SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of INTERSECT ALL"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t INTERSECT ALL SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of INTERSECT ALL"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t INTERSECT SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of INTERSECT"
        );

        IgniteTestUtils.assertThrows(
                CalciteContextException.class,
                () -> assertPlan("SELECT c1 FROM t INTERSECT SELECT c2 FROM t", schema, anything -> true),
                "Type mismatch in column 1 of INTERSECT"
        );
    }
}
