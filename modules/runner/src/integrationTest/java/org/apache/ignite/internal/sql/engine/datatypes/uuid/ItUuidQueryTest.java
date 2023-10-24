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
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseQueryDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.apache.ignite.internal.sql.engine.datatypes.tests.TestTypeArguments;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@code SELECT} operations for {@link UuidType UUID data type}.
 */
public class ItUuidQueryTest extends BaseQueryDataTypeTest<UUID> {
    @Override
    protected int initialNodes() {
        return 1;
    }

    /**
     * {@code UUID} vs type that can not be casted to {@code UUID}.
     */
    @ParameterizedTest
    @MethodSource("binaryComparisonOperators")
    public void testInvalidComparisonOperation(String opSql) {
        String query = format("SELECT * FROM t WHERE test_key {} 1", opSql);

        String error = format("Values passed to {} operator must have compatible types", opSql);

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, error, () -> checkQuery(query).check());
    }

    /** Test for equality predicate with dynamic parameter of compatible type is illegal. */
    @ParameterizedTest
    @MethodSource("convertedFrom")
    public void testEqConditionWithDynamicParameters(TestTypeArguments arguments) {
        UUID value1 = values.get(0);

        runSql("INSERT INTO t VALUES(1, ?)", value1);

        Executable qry = () -> {
            checkQuery("SELECT id FROM t where test_key = ? ORDER BY id")
                    .withParams(arguments.argValue(0))
                    .returns(1)
                    .returns(3)
                    .check();
        };

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Values passed to = operator must have compatible types", qry);
    }

    /** {@inheritDoc} **/
    @Override
    protected DataTypeTestSpec<UUID> getTypeSpec() {
        return DataTypeTestSpecs.UUID_TYPE;
    }
}
