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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;
import org.apache.ignite.internal.client.sql.AllowedQueryType;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for {@link ClientSqlCommonTest}.
 */
public class ClientSqlCommonTest {
    @ParameterizedTest
    @EnumSource(AllowedQueryType.class)
    void testConvertAllowedTypeToQueryType(AllowedQueryType type) {
        Set<SqlQueryType> sqlQueryTypes = ClientSqlCommon.convertAllowedTypeToQueryType(type);

        assertFalse(sqlQueryTypes.isEmpty());

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

                case ALLOW_MULTISTATEMENT_RESULT:
                    assertFalse(sqlQueryType.supportsIndependentExecution());
                    break;

                default:
                    fail("Unsupported type " + type);
            }
        });
    }
}
