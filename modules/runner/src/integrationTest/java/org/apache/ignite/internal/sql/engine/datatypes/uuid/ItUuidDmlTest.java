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

package org.apache.ignite.internal.sql.engine.datatypes.uuid;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;

import java.util.UUID;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseDmlDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.apache.ignite.internal.sql.engine.datatypes.tests.TestTypeArguments;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for DML statements for {@link UuidType UUID data type}.
 */
public class ItUuidDmlTest extends BaseDmlDataTypeTest<UUID> {
    /** {@code INSERT} with dynamic parameters is not allowed for compatible types. */
    @ParameterizedTest
    @MethodSource("convertedFrom")
    public void testInsertFromDynamicParameterFromConvertible(TestTypeArguments arguments) {
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Values passed to VALUES operator must have compatible types",
                () -> runSql("INSERT INTO t VALUES (1, ?)", arguments.argValue(0)));
    }

    /** {@code UPDATE} is not allowed for dynamic parameter of compatible type. */
    @ParameterizedTest
    @MethodSource("convertedFrom")
    public void testUpdateFromDynamicParameterFromConvertible(TestTypeArguments arguments) {
        String insert = format("INSERT INTO t VALUES (1, {})", arguments.valueExpr(0));
        runSql(insert);

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Dynamic parameter requires adding explicit type cast", () -> {
            checkQuery("UPDATE t SET test_key = ? WHERE id=1")
                    .withParams(arguments.argValue(0))
                    .returns(1L)
                    .check();
        });
    }

    /** Type mismatch in {@code INSERT}s {@code VALUES}.*/
    @ParameterizedTest
    @MethodSource("convertedFrom")
    public void testDisallowMismatchTypesOnInsert(TestTypeArguments arguments) {
        var query = format("INSERT INTO t (id, test_key) VALUES (10, null), (20, {})", arguments.valueExpr(0));
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Values passed to VALUES operator must have compatible types",
                () -> runSql(query));
    }

    /**
     * Type mismatch in {@code INSERT}s {@code VALUES} with dynamic parameters.
     */
    @ParameterizedTest
    @MethodSource("convertedFrom")
    public void testDisallowMismatchTypesOnInsertDynamicParam(TestTypeArguments arguments) {
        Object value1 = arguments.argValue(0);

        var query = "INSERT INTO t (id, test_key) VALUES (1, null), (2, ?)";
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Values passed to VALUES operator must have compatible types",
                () -> runSql(query, value1));
    }

    /** {@inheritDoc} **/
    @Override
    protected DataTypeTestSpec<UUID> getTypeSpec() {
        return DataTypeTestSpecs.UUID_TYPE;
    }
}
