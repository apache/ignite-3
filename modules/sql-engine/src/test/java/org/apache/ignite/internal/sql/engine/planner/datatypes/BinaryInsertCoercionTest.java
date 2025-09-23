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

import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.sql.type.SqlTypeName.BINARY_TYPES;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.generateValueByType;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.native2relationalType;

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.BinaryPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for INSERT operations, when values belongs to the BINARY type family.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which values.
 */
public class BinaryInsertCoercionTest extends BaseTypeCoercionTest {
    private static final NativeType DYN_VARBINARY_TYPE = NativeTypes.blobOf(PRECISION_NOT_SPECIFIED);

    @ParameterizedTest
    @MethodSource("args")
    public void insert(
            TypePair pair,
            Matcher<RexNode> operandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        String byteVal = generateLiteral(pair.second(), false);

        String sql = "INSERT INTO T VALUES(" + byteVal + ", " + byteVal + ")";
        assertPlan(sql, schema, keyValOperandMatcher(operandMatcher)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("argsDyn")
    public void insertDynamicParameters(
            TypePair pair,
            Matcher<RexNode> operandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        Object byteVal = generateValueByType(pair.second());

        assertPlan("INSERT INTO T VALUES(?, ?)", schema, keyValOperandMatcher(operandMatcher)::matches, List.of(byteVal, byteVal));
    }

    /**
     * This test ensures that {@link #args()} and {@link #argsDyn()} doesn't miss any type pair from {@link BinaryPair}.
     */
    @Test
    void insertArgsIncludesAllTypePairs() {
        checkIncludesAllTypePairs(args(), BinaryPair.class);
        checkIncludesAllTypePairs(argsDyn(), BinaryPair.class);
    }

    private static Stream<Arguments> args() {
        return Stream.of(
                forTypePair(BinaryPair.VARBINARY_1_VARBINARY_1)
                        .opMatches(ofBinaryTypeWithoutCast(Types.VARBINARY_1)),

                forTypePair(BinaryPair.VARBINARY_1_VARBINARY_2)
                        .opMatches(ofBinaryTypeWithoutCast(Types.VARBINARY_2)),

                forTypePair(BinaryPair.VARBINARY_1_VARBINARY_128)
                        .opMatches(ofBinaryTypeWithoutCast(Types.VARBINARY_128)),

                forTypePair(BinaryPair.VARBINARY_2_VARBINARY_2)
                        .opMatches(ofBinaryTypeWithoutCast(Types.VARBINARY_2)),

                forTypePair(BinaryPair.VARBINARY_2_VARBINARY_128)
                        .opMatches(ofBinaryTypeWithoutCast(Types.VARBINARY_128)),

                forTypePair(BinaryPair.VARBINARY_128_VARBINARY_128)
                        .opMatches(ofBinaryTypeWithoutCast(Types.VARBINARY_128))
        );
    }

    private static Stream<Arguments> argsDyn() {
        return Stream.of(
                forTypePair(BinaryPair.VARBINARY_1_VARBINARY_1)
                        .opMatches(ofBinaryTypeWithoutCast(DYN_VARBINARY_TYPE)),

                forTypePair(BinaryPair.VARBINARY_1_VARBINARY_2)
                        .opMatches(ofBinaryTypeWithoutCast(DYN_VARBINARY_TYPE)),

                forTypePair(BinaryPair.VARBINARY_1_VARBINARY_128)
                        .opMatches(ofBinaryTypeWithoutCast(DYN_VARBINARY_TYPE)),

                forTypePair(BinaryPair.VARBINARY_2_VARBINARY_2)
                        .opMatches(ofBinaryTypeWithoutCast(DYN_VARBINARY_TYPE)),

                forTypePair(BinaryPair.VARBINARY_2_VARBINARY_128)
                        .opMatches(ofBinaryTypeWithoutCast(DYN_VARBINARY_TYPE)),

                forTypePair(BinaryPair.VARBINARY_128_VARBINARY_128)
                        .opMatches(ofBinaryTypeWithoutCast(DYN_VARBINARY_TYPE))
        );
    }

    private static Matcher<RexNode> ofBinaryTypeWithoutCast(NativeType type) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();
        RelDataType sqlType = native2relationalType(typeFactory, type);

        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                return (BINARY_TYPES.contains(((RexNode) actual).getType().getSqlTypeName())
                        && BINARY_TYPES.contains(sqlType.getSqlTypeName())
                        && !((RexNode) actual).isA(SqlKind.CAST)
                        && ((RexNode) actual).getType().getPrecision() == ((VarlenNativeType) type).length());
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                description.appendText("was ").appendValue(item).appendText(" of type " + ((RexNode) item).getType());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(format("Operand of type {} that is not CAST", sqlType));
            }
        };
    }
}
