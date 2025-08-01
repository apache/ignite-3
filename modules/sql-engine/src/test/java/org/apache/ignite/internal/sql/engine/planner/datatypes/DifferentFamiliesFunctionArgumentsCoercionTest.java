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
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.native2relationalType;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.DifferentFamiliesPair;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for function arguments, when values belongs to the different type families.
 */
@SuppressWarnings({"ThrowableNotThrown", "DataFlowIssue"})
public class DifferentFamiliesFunctionArgumentsCoercionTest extends BaseTypeCoercionTest {
    private static final IgniteSchema SCHEMA = createSchema();

    private static final List<String> FUNCTIONS_WITH_NUMERIC_PARAM = List.of(
            "SUBSTRING('abc', {})", "POSITION('a' IN 'abc' FROM {})", "EXP({})", "POWER({}, 1)",
            "TRUNCATE({})", "SUBSTR('abc', {})", "ROUND({})"
    );

    private static final List<String> FUNCTIONS_WITH_CHARACTER_PARAM = List.of(
            "UPPER({})", "LOWER({})", "INITCAP({})", "CHARACTER_LENGTH({})", "REVERSE({})"
    );

    private static final List<String> FUNCTIONS_WITH_DATETIME_PARAM = List.of(
            "LAST_DAY({})", "MONTHNAME({})", "EXTRACT(day FROM {})", "UNIX_MILLIS({})"
    );

    private static final List<String> FUNCTIONS_WITH_BOOLEAN_PARAM = List.of(
            "EVERY({})", "SOME({})"
    );

    private static final List<String> FUNCTIONS_WITH_BINARY_PARAM = List.of(
            "TO_BASE64({})", "MD5({})", "SUBSTRING({} FROM 1)", "LEFT({}, 1)", "LENGTH({})"
    );

    @ParameterizedTest
    @MethodSource("nonNumericTypes")
    public void numericFunctionWithLiteralValue(NativeType type) {
        Assumptions.assumeTrue(
                type != NativeTypes.UUID,
                "Literal of type UUID is not supported"
        );

        String literal = generateLiteral(type, true);

        for (String function : FUNCTIONS_WITH_NUMERIC_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, literal), SCHEMA, anything -> true),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonNumericTypes")
    public void numericFunctionWithDynParam(NativeType type) {
        Object value = SqlTestUtils.generateValueByType(type);

        for (String function : FUNCTIONS_WITH_NUMERIC_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, "?"), SCHEMA, anything -> true, List.of(value)),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonNumericTypes")
    public void numericFunctionWithColumn(NativeType type) {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        for (String function : FUNCTIONS_WITH_NUMERIC_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, "c1") + " FROM t", schema, anything -> true),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonCharacterTypes")
    public void characterFunctionWithLiteralValue(NativeType type) {
        Assumptions.assumeTrue(
                type != NativeTypes.UUID,
                "Literal of type UUID is not supported"
        );

        String literal = generateLiteral(type, true);

        for (String function : FUNCTIONS_WITH_CHARACTER_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, literal), SCHEMA, anything -> true),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonCharacterTypes")
    public void characterFunctionWithDynParam(NativeType type) {
        Object value = SqlTestUtils.generateValueByType(type);

        for (String function : FUNCTIONS_WITH_CHARACTER_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, "?"), SCHEMA, anything -> true, List.of(value)),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonCharacterTypes")
    public void characterFunctionWithColumn(NativeType type) {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        for (String function : FUNCTIONS_WITH_CHARACTER_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, "c1") + " FROM t", schema, anything -> true),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonDatetimeTypes")
    public void dateTimeFunctionWithLiteralValue(NativeType type) {
        Assumptions.assumeTrue(
                type != NativeTypes.UUID,
                "Literal of type UUID is not supported"
        );

        String literal = generateLiteral(type, true);

        for (String function : FUNCTIONS_WITH_DATETIME_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, literal), SCHEMA, anything -> true),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonDatetimeTypes")
    public void dateTimeFunctionWithDynParam(NativeType type) {
        Object value = SqlTestUtils.generateValueByType(type);

        for (String function : FUNCTIONS_WITH_DATETIME_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, "?"), SCHEMA, anything -> true, List.of(value)),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonDatetimeTypes")
    public void dateTimeFunctionWithColumn(NativeType type) {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        for (String function : FUNCTIONS_WITH_DATETIME_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, "c1") + " FROM t", schema, anything -> true),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonBooleanTypes")
    public void booleanFunctionWithLiteralValue(NativeType type) {
        Assumptions.assumeTrue(
                type != NativeTypes.UUID,
                "Literal of type UUID is not supported"
        );

        String literal = generateLiteral(type, true);

        for (String function : FUNCTIONS_WITH_BOOLEAN_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, literal), SCHEMA, anything -> true),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonBooleanTypes")
    public void booleanFunctionWithDynParam(NativeType type) {
        Object value = SqlTestUtils.generateValueByType(type);

        for (String function : FUNCTIONS_WITH_BOOLEAN_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, "?"), SCHEMA, anything -> true, List.of(value)),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonBooleanTypes")
    public void booleanFunctionWithColumn(NativeType type) {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        for (String function : FUNCTIONS_WITH_BOOLEAN_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, "c1") + " FROM t", schema, anything -> true),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonBinaryTypes")
    public void binaryFunctionWithLiteralValue(NativeType type) {
        Assumptions.assumeTrue(
                type.spec() != ColumnType.STRING,
                "There is function accepting BINARY STRING but not accepting CHARACTER STRING"
        );

        Assumptions.assumeTrue(
                type != NativeTypes.UUID,
                "Literal of type UUID is not supported"
        );

        String literal = generateLiteral(type, true);

        for (String function : FUNCTIONS_WITH_BINARY_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, literal), SCHEMA, anything -> true),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonBinaryTypes")
    public void binaryFunctionWithDynParam(NativeType type) {
        Assumptions.assumeTrue(
                type.spec() != ColumnType.STRING,
                "There is function accepting BINARY STRING but not accepting CHARACTER STRING"
        );

        Object value = SqlTestUtils.generateValueByType(type);

        for (String function : FUNCTIONS_WITH_BINARY_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, "?"), SCHEMA, anything -> true, List.of(value)),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("nonBinaryTypes")
    public void binaryFunctionWithColumn(NativeType type) {
        Assumptions.assumeTrue(
                type.spec() != ColumnType.STRING,
                "There is function accepting BINARY STRING but not accepting CHARACTER STRING"
        );

        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        for (String function : FUNCTIONS_WITH_BINARY_PARAM) {
            IgniteTestUtils.assertThrows(
                    CalciteContextException.class,
                    () -> assertPlan("SELECT " + format(function, "c1") + " FROM t", schema, anything -> true),
                    format("Cannot apply '{}' to arguments of type", function.substring(0, function.indexOf('(')))
            );
        }
    }

    private static Stream<Arguments> nonNumericTypes() {
        return allTypes()
                .filter(nonFamilyType(SqlTypeFamily.NUMERIC))
                .map(Arguments::of);
    }

    private static Stream<Arguments> nonCharacterTypes() {
        return allTypes()
                .filter(nonFamilyType(SqlTypeFamily.CHARACTER))
                .map(Arguments::of);
    }

    private static Stream<Arguments> nonDatetimeTypes() {
        return allTypes()
                .filter(nonFamilyType(SqlTypeFamily.DATETIME))
                .map(Arguments::of);
    }

    private static Stream<Arguments> nonBooleanTypes() {
        return allTypes()
                .filter(nonFamilyType(SqlTypeFamily.BOOLEAN))
                .map(Arguments::of);
    }

    private static Stream<Arguments> nonBinaryTypes() {
        return allTypes()
                .filter(nonFamilyType(SqlTypeFamily.BINARY))
                .map(Arguments::of);
    }

    private static Stream<NativeType> allTypes() {
        return Arrays.stream(DifferentFamiliesPair.values())
                .flatMap(pair -> Stream.of(pair.first(), pair.second()))
                .distinct();
    }

    private static Predicate<NativeType> nonFamilyType(SqlTypeFamily family) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        return type -> {
            RelDataType relType = native2relationalType(typeFactory, type);

            return !family.contains(relType);
        };
    } 
}
