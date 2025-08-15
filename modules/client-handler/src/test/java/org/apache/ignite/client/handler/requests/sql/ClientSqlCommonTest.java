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

package org.apache.ignite.client.handler.requests.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.sql.QueryModifier;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for {@link ClientSqlCommonTest}.
 */
public class ClientSqlCommonTest {
    @ParameterizedTest
    @EnumSource(QueryModifier.class)
    void testConvertQueryModifierToQueryType(QueryModifier type) {
        Set<SqlQueryType> sqlQueryTypes = ClientSqlCommon.convertQueryModifierToQueryType(Set.of(type));

        assertThat(sqlQueryTypes.isEmpty(), is(type == QueryModifier.ALLOW_MULTISTATEMENT));

        sqlQueryTypes.forEach(sqlQueryType -> {
            switch (type) {
                case ALLOW_ROW_SET_RESULT:
                    assertTrue(sqlQueryType.hasRowSet());
                    break;

                case ALLOW_AFFECTED_ROWS_RESULT:
                    assertTrue(sqlQueryType.returnsAffectedRows());
                    break;

                case ALLOW_APPLIED_RESULT:
                    assertTrue(sqlQueryType.supportsWasApplied());
                    break;

                case ALLOW_TX_CONTROL:
                    assertFalse(sqlQueryType.supportsIndependentExecution());
                    break;

                default:
                    fail("Unsupported type " + type);
            }
        });
    }

    @Test
    void testAllQueryTypesCoveredByQueryModifiers() {
        Set<SqlQueryType> sqlQueryTypesFromModifiers = EnumSet.noneOf(SqlQueryType.class);

        for (QueryModifier modifier : QueryModifier.values()) {
            Set<SqlQueryType> queryTypes = ClientSqlCommon.convertQueryModifierToQueryType(Set.of(modifier));

            for (SqlQueryType queryType : queryTypes) {
                boolean added = sqlQueryTypesFromModifiers.add(queryType);

                assertTrue(added, "Duplicate type: " + queryType);
            }
        }

        Set<SqlQueryType> allQueryTypes = Arrays.stream(SqlQueryType.values()).collect(Collectors.toSet());

        allQueryTypes.removeAll(sqlQueryTypesFromModifiers);

        assertThat(allQueryTypes, is(empty()));
    }
}
