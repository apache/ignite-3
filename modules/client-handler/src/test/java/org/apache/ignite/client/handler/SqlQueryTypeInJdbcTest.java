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

package org.apache.ignite.client.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import org.apache.ignite.internal.sql.SqlQueryType;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SqlQueryType} usage in JDBC statements.
 */
public class SqlQueryTypeInJdbcTest extends BaseIgniteAbstractTest {

    @Test
    public void selectStatementQueries() {
        Set<SqlQueryType> types = Set.of(
                SqlQueryType.QUERY,
                SqlQueryType.EXPLAIN
        );

        assertEquals(types, JdbcQueryEventHandlerImpl.SELECT_STATEMENT_QUERIES);
    }

    @Test
    public void updateStatementQueries() {
        Set<SqlQueryType> types = Set.of(
                SqlQueryType.DDL,
                SqlQueryType.DML,
                SqlQueryType.KILL
        );

        assertEquals(types, JdbcQueryEventHandlerImpl.UPDATE_STATEMENT_QUERIES);
    }

    @Test
    public void zeroUpdateCountQueries() {
        Set<SqlQueryType> types = Set.of(
                SqlQueryType.DDL,
                SqlQueryType.KILL,
                SqlQueryType.TX_CONTROL
        );

        assertEquals(types, JdbcHandlerBase.ZERO_UPDATE_COUNT_QUERIES);
    }
}
