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
import org.apache.ignite.internal.sql.engine.util.VarBinary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for queries that use indexes with {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinaryIndexTest extends BaseIndexDataTypeTest<VarBinary> {

    @BeforeAll
    public void createTable() {
        runSql("CREATE TABLE t2(id INTEGER PRIMARY KEY, test_key VARBINARY(10))");
        runSql("CREATE INDEX t2_test_key_idx on t2 (test_key)");

        runSql("INSERT INTO t2 VALUES(1, $0)");
        runSql("INSERT INTO t2 VALUES(2, $1)");
        runSql("INSERT INTO t2 VALUES(3, $2)");

        runSql("CREATE TABLE t3(id INTEGER PRIMARY KEY, test_key BINARY(10))");
        runSql("CREATE INDEX t3_test_key_idx on t3 (test_key)");

        runSql("INSERT INTO t3 VALUES(1, $0)");
        runSql("INSERT INTO t3 VALUES(2, $1)");
        runSql("INSERT INTO t3 VALUES(3, $2)");
    }

    /**
     * Key lookup
     *
     * T.test_key - VARBINARY with default precision.
     * T2.test_key - VARBINARY with specific precision.
     * T3.test_key - BINARY with specific precision.
     */
    @ParameterizedTest
    @MethodSource("indexChecks")
    public void testKeyLookUp2(String table, ValueMode mode) {
        VarBinary value1 = values.get(0);
        String value1str = mode.toSql(testTypeSpec, value1);

        checkQuery(format("SELECT * FROM {} WHERE test_key = {}", table, value1str))
                .matches(containsIndexScan("PUBLIC", table, table + "_TEST_KEY_IDX"))
                .returns(1, value1)
                .check();
    }

    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<VarBinary> getTypeSpec() {
        return DataTypeTestSpecs.VARBINARY_TYPE;
    }

    private static Stream<Arguments> indexChecks() {
        return Stream.of(
                Arguments.of("T", ValueMode.LITERAL),
                //Arguments.of("T", ValueMode.CAST), duplicates a test case defined in the base class.
                Arguments.of("T", ValueMode.CAST_WITH_PRECISION),

                Arguments.of("T2", ValueMode.LITERAL),
                Arguments.of("T2", ValueMode.CAST),
                Arguments.of("T2", ValueMode.CAST_WITH_PRECISION),

                Arguments.of("T3", ValueMode.LITERAL),
                Arguments.of("T3", ValueMode.CAST),
                Arguments.of("T3", ValueMode.CAST_WITH_PRECISION)
        );
    }

    public enum ValueMode {
        LITERAL,
        CAST,
        CAST_WITH_PRECISION;

        <T extends Comparable<T>> String toSql(DataTypeTestSpec<T> spec, T value) {
            switch (this) {
                case LITERAL:
                    return spec.toLiteral(value);
                case CAST: {
                    String str = spec.toStringValue(value);
                    return format("'{}'::VARBINARY", str);
                }
                case CAST_WITH_PRECISION: {
                    String str = spec.toStringValue(value);
                    return format("'{}'::VARBINARY(8)", str);
                }
                default:
                    throw new IllegalArgumentException("Unexpected mode");
            }
        }
    }
}
