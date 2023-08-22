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


package org.apache.ignite.internal.sql.engine.datatypes.varbinary;

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.stream.Stream;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseIndexDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.apache.ignite.internal.sql.engine.datatypes.tests.TestTypeArguments;
import org.apache.ignite.internal.sql.engine.util.VarBinary;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for queries that use indexes with {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinaryIndexTest extends BaseIndexDataTypeTest<VarBinary> {

    @BeforeAll
    public void createTable() {
        runSql("CREATE TABLE binary_fixed_length(id INTEGER PRIMARY KEY, test_key BINARY(10))");
        runSql("CREATE INDEX binary_fixed_length_test_key_idx on binary_fixed_length (test_key)");

        runSql("INSERT INTO binary_fixed_length VALUES(1, $0)");
        runSql("INSERT INTO binary_fixed_length VALUES(2, $1)");
        runSql("INSERT INTO binary_fixed_length VALUES(3, $2)");
    }

    /**
     * Key lookup. Literal, cast, cast with precision against different column types.
     */
    @ParameterizedTest
    @MethodSource("indexChecks")
    public void testKeyLookUp2(String table, ValueMode mode) {
        VarBinary value1 = values.get(0);
        String value1str = mode.toSql(testTypeSpec, value1);

        // TODO Disable for VARBINARY, remove after https://issues.apache.org/jira/browse/IGNITE-19931 is fixed
        Assumptions.assumeFalse(mode == ValueMode.CAST);

        checkQuery(format("SELECT * FROM {} WHERE test_key = {}", table, value1str))
                .matches(containsIndexScan("PUBLIC", table, table + "_TEST_KEY_IDX"))
                .returns(1, value1)
                .check();
    }

    /** {@inheritDoc} */
    @ParameterizedTest
    @MethodSource("compoundIndex")
    @Override
    public void testCompoundIndex(TestTypeArguments<VarBinary> arguments) throws InterruptedException {
        // TODO Disable for VARBINARY, remove after https://issues.apache.org/jira/browse/IGNITE-19931 is fixed.
        // Lookups for VARBINARY and VARCHAR/CHAR work
        Assumptions.assumeFalse(arguments.toString().startsWith("VARBINARY"));

        super.testCompoundIndex(arguments);
    }

    /** {@inheritDoc} */
    @Test
    @Override
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19931")
    public void testIndexDynParam() {
        super.testIndexDynParam();
    }

    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<VarBinary> getTypeSpec() {
        return DataTypeTestSpecs.VARBINARY_TYPE;
    }

    private static Stream<Arguments> indexChecks() {
        return Stream.of(
                Arguments.of(Named.of("VARBINARY_DEFAULT_LENGTH", "T"), ValueMode.LITERAL),
                Arguments.of(Named.of("VARBINARY_DEFAULT_LENGTH", "T"), ValueMode.CAST_WITH_PRECISION),

                Arguments.of("BINARY_FIXED_LENGTH", ValueMode.LITERAL),
                Arguments.of("BINARY_FIXED_LENGTH", ValueMode.CAST),
                Arguments.of("BINARY_FIXED_LENGTH", ValueMode.CAST_WITH_PRECISION)
        );
    }

    /**
     * Value generation mode.
     */
    public enum ValueMode {
        LITERAL,
        CAST,
        CAST_WITH_PRECISION;

        String toSql(DataTypeTestSpec<VarBinary> spec, VarBinary value) {
            switch (this) {
                case LITERAL:
                    return spec.toLiteral(value);
                case CAST: {
                    String str = IgniteUtils.toHexString(value.get());
                    return format("x'{}'::VARBINARY", str);
                }
                case CAST_WITH_PRECISION: {
                    String str = IgniteUtils.toHexString(value.get());
                    return format("x'{}'::VARBINARY(8)", str);
                }
                default:
                    throw new IllegalArgumentException("Unexpected mode");
            }
        }
    }
}
