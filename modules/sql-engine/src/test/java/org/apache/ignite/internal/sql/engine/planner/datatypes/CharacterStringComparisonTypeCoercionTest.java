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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.CharacterStringPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.hamcrest.Matcher;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

/**
 * A set of tests to verify behavior of type coercion for binary comparison, when operands belongs to the CHARACTER STRING type family.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which operand.
 */
public class CharacterStringComparisonTypeCoercionTest extends BaseTypeCoercionTest {
    @ParameterizedTest
    @EnumSource(CharacterStringPair.class)
    public void equalsTo(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        Matcher<RexNode> firstOpMatcher = ofTypeWithoutCast(typePair.first());
        Matcher<RexNode> secondOpMatcher = ofTypeWithoutCast(typePair.second());

        assertPlan("SELECT c1 = c2 FROM t", schema, operandMatcher(firstOpMatcher, secondOpMatcher)::matches);
        assertPlan("SELECT c2 = c1 FROM t", schema, operandMatcher(secondOpMatcher, firstOpMatcher)::matches);
    }

    @ParameterizedTest
    @EnumSource(CharacterStringPair.class)
    public void equalsToDynamicParams(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        // Type of the dynamic param for String argument is always VARCHAR with default precision.
        Matcher<RexNode> opMatcher = ofTypeWithoutCast(Types.VARCHAR_DEFAULT);

        Object firstVal = SqlTestUtils.generateValueByType(typePair.first());
        Object secondVal = SqlTestUtils.generateValueByType(typePair.second());

        assert firstVal != null;
        assert secondVal != null;

        assertPlan("SELECT ? = ? FROM t", schema, operandMatcher(opMatcher, opMatcher)::matches,
                List.of(firstVal, secondVal));
        assertPlan("SELECT ? = ? FROM t", schema, operandMatcher(opMatcher, opMatcher)::matches,
                List.of(secondVal, firstVal));
    }

    @ParameterizedTest
    @EnumSource(
            value = CharacterStringPair.class,
            mode = Mode.EXCLUDE,
            // comparison with operands of equal types are reduced to boolean constant,
            // therefore all such pairs are excluded
            names = {"VARCHAR_1_VARCHAR_1", "VARCHAR_3_VARCHAR_3", "VARCHAR_5_VARCHAR_5"}
    )
    public void equalsToLiteral(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        // Type of string literal is always CHAR with precision equals to length of the string.
        Matcher<RexNode> firstOpMatcher = ofTypeWithoutCast(
                Commons.typeFactory().createSqlType(SqlTypeName.CHAR, ((VarlenNativeType) typePair.first()).length())
        );
        Matcher<RexNode> secondOpMatcher = ofTypeWithoutCast(
                Commons.typeFactory().createSqlType(SqlTypeName.CHAR, ((VarlenNativeType) typePair.second()).length())
        );

        String firstVal = generateLiteral(typePair.first(), false);
        String secondVal = generateLiteral(typePair.second(), false);

        assertPlan(
                format("SELECT {} = {} FROM t", firstVal, secondVal),
                schema,
                operandMatcher(firstOpMatcher, secondOpMatcher)::matches
        );
        assertPlan(
                format("SELECT {} = {} FROM t", secondVal, firstVal),
                schema,
                operandMatcher(secondOpMatcher, firstOpMatcher)::matches
        );
    }

    @ParameterizedTest
    @EnumSource(CharacterStringPair.class)
    public void lessThan(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        Matcher<RexNode> firstOpMatcher = ofTypeWithoutCast(typePair.first());
        Matcher<RexNode> secondOpMatcher = ofTypeWithoutCast(typePair.second());

        assertPlan("SELECT c1 < c2 FROM t", schema, operandMatcher(firstOpMatcher, secondOpMatcher)::matches);
        assertPlan("SELECT c2 < c1 FROM t", schema, operandMatcher(secondOpMatcher, firstOpMatcher)::matches);
    }

    @ParameterizedTest
    @EnumSource(CharacterStringPair.class)
    public void lessThanDynamicParams(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        // Type of the dynamic param for String argument is always VARCHAR with default precision.
        Matcher<RexNode> opMatcher = ofTypeWithoutCast(Types.VARCHAR_DEFAULT);

        Object firstVal = SqlTestUtils.generateValueByType(typePair.first());
        Object secondVal = SqlTestUtils.generateValueByType(typePair.second());

        assert firstVal != null;
        assert secondVal != null;

        assertPlan("SELECT ? < ? FROM t", schema, operandMatcher(opMatcher, opMatcher)::matches,
                List.of(firstVal, secondVal));
        assertPlan("SELECT ? < ? FROM t", schema, operandMatcher(opMatcher, opMatcher)::matches,
                List.of(secondVal, firstVal));
    }

    @ParameterizedTest
    @EnumSource(
            value = CharacterStringPair.class,
            mode = Mode.EXCLUDE,
            // comparison with operands of equal types are reduced to boolean constant,
            // therefore all such pairs are excluded
            names = {"VARCHAR_1_VARCHAR_1", "VARCHAR_3_VARCHAR_3", "VARCHAR_5_VARCHAR_5"}
    )
    public void lessThanLiteral(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        // Type of string literal is always CHAR with precision equals to length of the string.
        Matcher<RexNode> firstOpMatcher = ofTypeWithoutCast(
                Commons.typeFactory().createSqlType(SqlTypeName.CHAR, ((VarlenNativeType) typePair.first()).length())
        );
        Matcher<RexNode> secondOpMatcher = ofTypeWithoutCast(
                Commons.typeFactory().createSqlType(SqlTypeName.CHAR, ((VarlenNativeType) typePair.second()).length())
        );

        String firstVal = generateLiteral(typePair.first(), false);
        String secondVal = generateLiteral(typePair.second(), false);

        assertPlan(
                format("SELECT {} < {} FROM t", firstVal, secondVal),
                schema,
                operandMatcher(firstOpMatcher, secondOpMatcher)::matches
        );
        assertPlan(
                format("SELECT {} < {} FROM t", secondVal, firstVal),
                schema,
                operandMatcher(secondOpMatcher, firstOpMatcher)::matches
        );
    }

    @ParameterizedTest
    @EnumSource(CharacterStringPair.class)
    public void lessThanOrEqual(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        Matcher<RexNode> firstOpMatcher = ofTypeWithoutCast(typePair.first());
        Matcher<RexNode> secondOpMatcher = ofTypeWithoutCast(typePair.second());

        assertPlan("SELECT c1 <= c2 FROM t", schema, operandMatcher(firstOpMatcher, secondOpMatcher)::matches);
        assertPlan("SELECT c2 <= c1 FROM t", schema, operandMatcher(secondOpMatcher, firstOpMatcher)::matches);
    }

    @ParameterizedTest
    @EnumSource(CharacterStringPair.class)
    public void lessThanOrEqualDynamicParams(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        // Type of the dynamic param for String argument is always VARCHAR with default precision.
        Matcher<RexNode> opMatcher = ofTypeWithoutCast(Types.VARCHAR_DEFAULT);

        Object firstVal = SqlTestUtils.generateValueByType(typePair.first());
        Object secondVal = SqlTestUtils.generateValueByType(typePair.second());

        assert firstVal != null;
        assert secondVal != null;

        assertPlan("SELECT ? <= ? FROM t", schema, operandMatcher(opMatcher, opMatcher)::matches,
                List.of(firstVal, secondVal));
        assertPlan("SELECT ? <= ? FROM t", schema, operandMatcher(opMatcher, opMatcher)::matches,
                List.of(secondVal, firstVal));
    }

    @ParameterizedTest
    @EnumSource(
            value = CharacterStringPair.class,
            mode = Mode.EXCLUDE,
            // comparison with operands of equal types are reduced to boolean constant,
            // therefore all such pairs are excluded
            names = {"VARCHAR_1_VARCHAR_1", "VARCHAR_3_VARCHAR_3", "VARCHAR_5_VARCHAR_5"}
    )
    public void lessThanOrEqualLiteral(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        // Type of string literal is always CHAR with precision equals to length of the string.
        Matcher<RexNode> firstOpMatcher = ofTypeWithoutCast(
                Commons.typeFactory().createSqlType(SqlTypeName.CHAR, ((VarlenNativeType) typePair.first()).length())
        );
        Matcher<RexNode> secondOpMatcher = ofTypeWithoutCast(
                Commons.typeFactory().createSqlType(SqlTypeName.CHAR, ((VarlenNativeType) typePair.second()).length())
        );

        String firstVal = generateLiteral(typePair.first(), false);
        String secondVal = generateLiteral(typePair.second(), false);

        assertPlan(
                format("SELECT {} <= {} FROM t", firstVal, secondVal),
                schema,
                operandMatcher(firstOpMatcher, secondOpMatcher)::matches
        );
        assertPlan(
                format("SELECT {} <= {} FROM t", secondVal, firstVal),
                schema,
                operandMatcher(secondOpMatcher, firstOpMatcher)::matches
        );
    }

    @ParameterizedTest
    @EnumSource(CharacterStringPair.class)
    public void greaterThan(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        Matcher<RexNode> firstOpMatcher = ofTypeWithoutCast(typePair.first());
        Matcher<RexNode> secondOpMatcher = ofTypeWithoutCast(typePair.second());

        assertPlan("SELECT c1 > c2 FROM t", schema, operandMatcher(firstOpMatcher, secondOpMatcher)::matches);
        assertPlan("SELECT c2 > c1 FROM t", schema, operandMatcher(secondOpMatcher, firstOpMatcher)::matches);
    }

    @ParameterizedTest
    @EnumSource(CharacterStringPair.class)
    public void greaterThanDynamicParams(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        // Type of the dynamic param for String argument is always VARCHAR with default precision.
        Matcher<RexNode> opMatcher = ofTypeWithoutCast(Types.VARCHAR_DEFAULT);

        Object firstVal = SqlTestUtils.generateValueByType(typePair.first());
        Object secondVal = SqlTestUtils.generateValueByType(typePair.second());

        assert firstVal != null;
        assert secondVal != null;

        assertPlan("SELECT ? > ? FROM t", schema, operandMatcher(opMatcher, opMatcher)::matches,
                List.of(firstVal, secondVal));
        assertPlan("SELECT ? > ? FROM t", schema, operandMatcher(opMatcher, opMatcher)::matches,
                List.of(secondVal, firstVal));
    }

    @ParameterizedTest
    @EnumSource(
            value = CharacterStringPair.class,
            mode = Mode.EXCLUDE,
            // comparison with operands of equal types are reduced to boolean constant,
            // therefore all such pairs are excluded
            names = {"VARCHAR_1_VARCHAR_1", "VARCHAR_3_VARCHAR_3", "VARCHAR_5_VARCHAR_5"}
    )
    public void greaterThanLiteral(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        // Type of string literal is always CHAR with precision equals to length of the string.
        Matcher<RexNode> firstOpMatcher = ofTypeWithoutCast(
                Commons.typeFactory().createSqlType(SqlTypeName.CHAR, ((VarlenNativeType) typePair.first()).length())
        );
        Matcher<RexNode> secondOpMatcher = ofTypeWithoutCast(
                Commons.typeFactory().createSqlType(SqlTypeName.CHAR, ((VarlenNativeType) typePair.second()).length())
        );

        String firstVal = generateLiteral(typePair.first(), false);
        String secondVal = generateLiteral(typePair.second(), false);

        assertPlan(
                format("SELECT {} > {} FROM t", firstVal, secondVal),
                schema,
                operandMatcher(firstOpMatcher, secondOpMatcher)::matches
        );
        assertPlan(
                format("SELECT {} > {} FROM t", secondVal, firstVal),
                schema,
                operandMatcher(secondOpMatcher, firstOpMatcher)::matches
        );
    }

    @ParameterizedTest
    @EnumSource(CharacterStringPair.class)
    public void greaterThanOrEqual(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        Matcher<RexNode> firstOpMatcher = ofTypeWithoutCast(typePair.first());
        Matcher<RexNode> secondOpMatcher = ofTypeWithoutCast(typePair.second());

        assertPlan("SELECT c1 >= c2 FROM t", schema, operandMatcher(firstOpMatcher, secondOpMatcher)::matches);
        assertPlan("SELECT c2 >= c1 FROM t", schema, operandMatcher(secondOpMatcher, firstOpMatcher)::matches);
    }

    @ParameterizedTest
    @EnumSource(CharacterStringPair.class)
    public void greaterThanOrEqualDynamicParams(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        // Type of the dynamic param for String argument is always VARCHAR with default precision.
        Matcher<RexNode> opMatcher = ofTypeWithoutCast(Types.VARCHAR_DEFAULT);

        Object firstVal = SqlTestUtils.generateValueByType(typePair.first());
        Object secondVal = SqlTestUtils.generateValueByType(typePair.second());

        assert firstVal != null;
        assert secondVal != null;

        assertPlan("SELECT ? >= ? FROM t", schema, operandMatcher(opMatcher, opMatcher)::matches,
                List.of(firstVal, secondVal));
        assertPlan("SELECT ? >= ? FROM t", schema, operandMatcher(opMatcher, opMatcher)::matches,
                List.of(secondVal, firstVal));
    }

    @ParameterizedTest
    @EnumSource(
            value = CharacterStringPair.class,
            mode = Mode.EXCLUDE,
            // comparison with operands of equal types are reduced to boolean constant,
            // therefore all such pairs are excluded
            names = {"VARCHAR_1_VARCHAR_1", "VARCHAR_3_VARCHAR_3", "VARCHAR_5_VARCHAR_5"}
    )
    public void greaterThanOrEqualLiteral(
            TypePair typePair
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        // Type of string literal is always CHAR with precision equals to length of the string.
        Matcher<RexNode> firstOpMatcher = ofTypeWithoutCast(
                Commons.typeFactory().createSqlType(SqlTypeName.CHAR, ((VarlenNativeType) typePair.first()).length())
        );
        Matcher<RexNode> secondOpMatcher = ofTypeWithoutCast(
                Commons.typeFactory().createSqlType(SqlTypeName.CHAR, ((VarlenNativeType) typePair.second()).length())
        );

        String firstVal = generateLiteral(typePair.first(), false);
        String secondVal = generateLiteral(typePair.second(), false);

        assertPlan(
                format("SELECT {} >= {} FROM t", firstVal, secondVal),
                schema,
                operandMatcher(firstOpMatcher, secondOpMatcher)::matches
        );
        assertPlan(
                format("SELECT {} >= {} FROM t", secondVal, firstVal),
                schema,
                operandMatcher(secondOpMatcher, firstOpMatcher)::matches
        );
    }
}
