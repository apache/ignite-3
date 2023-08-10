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

import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseQueryDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@code SELECT} operations for {@link UuidType UUID data type}.
 */
public class ItUuidQueryTest extends BaseQueryDataTypeTest<UUID> {

    /**
     * {@code UUID} vs type that can not be casted to {@code UUID}.
     */
    @ParameterizedTest
    @MethodSource("binaryComparisonOperators")
    public void testInvalidComparisonOperation(String opSql) {
        String query = format("SELECT * FROM t WHERE test_key {} 1", opSql);

        IgniteException t = assertThrows(IgniteException.class, () -> checkQuery(query).check());
        String error = format("Values passed to {} operator must have compatible types", opSql);
        assertThat(t.getMessage(), containsString(error));
    }

    /** {@inheritDoc} **/
    @Override
    protected DataTypeTestSpec<UUID> getTypeSpec() {
        return DataTypeTestSpecs.UUID_TYPE;
    }
}
