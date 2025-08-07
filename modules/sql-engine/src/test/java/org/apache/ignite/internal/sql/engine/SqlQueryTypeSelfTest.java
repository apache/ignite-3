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

package org.apache.ignite.internal.sql.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.sql.SqlQueryType;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for {@link SqlQueryType}.
 */
public class SqlQueryTypeSelfTest extends BaseIgniteAbstractTest {

    @ParameterizedTest
    @EnumSource(SqlQueryType.class)
    public void supportsParseResultCaching(SqlQueryType queryType) {
        Set<SqlQueryType> types = Set.of(
                SqlQueryType.QUERY,
                SqlQueryType.DML
        );
        assertEquals(types.contains(queryType), queryType.supportsParseResultCaching(),  queryType.name());
    }

    @ParameterizedTest
    @EnumSource(SqlQueryType.class)
    public void supportsExplain(SqlQueryType queryType) {
        Set<SqlQueryType> types = Set.of(
                SqlQueryType.QUERY,
                SqlQueryType.DML
        );
        assertEquals(types.contains(queryType), queryType.supportsExplain(), queryType.name());
    }

    @ParameterizedTest
    @EnumSource(SqlQueryType.class)
    public void supportsExplicitTransactions(SqlQueryType queryType) {
        // OptimizingPhaseHandler ensureStatementMatchesTx

        Set<SqlQueryType> types = Set.of(
                SqlQueryType.QUERY,
                SqlQueryType.DML,
                SqlQueryType.EXPLAIN,
                SqlQueryType.KILL
        );
        assertEquals(types.contains(queryType), queryType.supportsExplicitTransactions(), queryType.name());
    }

    @ParameterizedTest
    @EnumSource(SqlQueryType.class)
    public void supportsTransactions(SqlQueryType queryType) {
        // ScriptTransactionContext registerCursorFuture

        Set<SqlQueryType> types = Set.of(
                SqlQueryType.QUERY,
                SqlQueryType.DML
        );
        assertEquals(types.contains(queryType), queryType.supportsTransactions(), queryType.name());
    }

    @ParameterizedTest
    @EnumSource(SqlQueryType.class)
    public void supportsIndependentExecution(SqlQueryType queryType) {
        // ValidationHelper validateParsedStatement

        Set<SqlQueryType> types = Set.of(
                SqlQueryType.QUERY,
                SqlQueryType.DML,
                SqlQueryType.DDL,
                SqlQueryType.EXPLAIN,
                SqlQueryType.KILL
        );
        assertEquals(types.contains(queryType), queryType.supportsIndependentExecution(), queryType.name());

        Set<SqlQueryType> singleStmtTypes = SqlQueryType.SINGLE_STMT_TYPES;
        assertEquals(singleStmtTypes.contains(queryType), types.contains(queryType), queryType.name()
                + " SINGLE_STMT_TYPES/standaloneStatement do not match");
    }

    @ParameterizedTest
    @EnumSource(SqlQueryType.class)
    public void implicitTransactionReadOnlyMode(SqlQueryType queryType) {
        // ExecutionService executeQuery
        // ExecutionService executeExecutablePlan
        Map<SqlQueryType, Boolean> implicitTxReadOnlyMode = Map.of(
                SqlQueryType.QUERY, true,
                SqlQueryType.EXPLAIN, true,
                SqlQueryType.DML, false
        );

        if (implicitTxReadOnlyMode.containsKey(queryType)) {
            assertEquals(queryType.implicitTransactionReadOnlyMode(), implicitTxReadOnlyMode.get(queryType));
        } else {
            assertThrows(UnsupportedOperationException.class,
                    queryType::implicitTransactionReadOnlyMode,
                    queryType.name() + " should not support this method"
            );
        }
    }

    @ParameterizedTest
    @EnumSource(SqlQueryType.class)
    public void hasRowSet(SqlQueryType queryType) {
        // AsyncResultSetImpl hasRowSet

        Set<SqlQueryType> types = Set.of(
                SqlQueryType.QUERY,
                SqlQueryType.EXPLAIN
        );
        assertEquals(types.contains(queryType), queryType.hasRowSet(), queryType.name());
    }

    @ParameterizedTest
    @EnumSource(SqlQueryType.class)
    public void returnsAffectedRows(SqlQueryType queryType) {
        // AsyncResultSetImpl affectedRows

        Set<SqlQueryType> types = Set.of(
                SqlQueryType.DML
        );
        assertEquals(types.contains(queryType), queryType.returnsAffectedRows(), queryType.name());
    }

    @ParameterizedTest
    @EnumSource(SqlQueryType.class)
    public void supportsWasApplied(SqlQueryType queryType) {
        // AsyncResultSetImpl wasApplied
        Set<SqlQueryType> types = Set.of(
                SqlQueryType.DDL,
                SqlQueryType.KILL
        );
        assertEquals(types.contains(queryType), queryType.supportsWasApplied(), queryType.name());
    }
}
