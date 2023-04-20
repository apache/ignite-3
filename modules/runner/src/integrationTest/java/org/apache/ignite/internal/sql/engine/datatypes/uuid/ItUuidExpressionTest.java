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
import org.apache.ignite.internal.sql.engine.datatypes.CustomDataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseExpressionCustomDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.CustomDataTypeTestSpec;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for expressions for {@link UuidType UUID data type}.
 */
public class ItUuidExpressionTest extends BaseExpressionCustomDataTypeTest<UUID> {

    /** {@code RAND_UUID} function.*/
    @Test
    public void testRand() {
        checkQuery("SELECT RAND_UUID()").columnTypes(UUID.class).check();
    }

    /**
     * {@code RAND} function returns different results.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18931")
    public void testRandomUuidComparison() {
        assertQuery("SELECT RAND_UUID() = RAND_UUID()").returns(false).check();
        assertQuery("SELECT RAND_UUID() != RAND_UUID()").returns(true).check();
    }

    /**
     * {@code UUID} vs type that can not be casted to {@code UUID}.
     */
    @ParameterizedTest
    @MethodSource("binaryComparisonOperators")
    public void testUuidInvalidOperationsDynamicParams(String op) {
        var query = format("SELECT ? {} 1", op);
        var t = assertThrows(IgniteException.class, () -> sql(query, dataSamples.min()));
        assertThat(t.getMessage(), containsString("class java.util.UUID cannot be cast to class java.lang.Integer"));
    }

    /** {@inheritDoc} **/
    @Override
    protected CustomDataTypeTestSpec<UUID> getTypeSpec() {
        return CustomDataTypeTestSpecs.UUID_TYPE;
    }
}
