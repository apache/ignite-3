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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseQueryDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.apache.ignite.internal.sql.engine.util.VarBinary;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@code SELECT} statement for {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinaryQueryTest extends BaseQueryDataTypeTest<VarBinary> {

    /**
     * {@code VARCHAR} vs type that can not be casted to {@code VARCHAR}.
     */
    @ParameterizedTest
    @MethodSource("binaryComparisonOperators")
    public void testInvalidComparisonOperation(String opSql) {
        String query = format("SELECT * FROM t WHERE test_key {} 1", opSql);

        IgniteException t = assertThrows(IgniteException.class, () -> checkQuery(query).check());
        String error = format("Values passed to {} operator must have compatible types", opSql);
        assertThat(t.getMessage(), containsString(error));
    }

    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<VarBinary> getTypeSpec() {
        return DataTypeTestSpecs.VARBINARY_TYPE;
    }
}
