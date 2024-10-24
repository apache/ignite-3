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

package org.apache.ignite.internal.sql.engine.registry;

import static org.apache.ignite.internal.sql.engine.registry.RunningQueriesRegistryImpl.SCRIPT_QUERY_TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.framework.ExplicitTxContext;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransaction;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContextImpl;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.ScriptTransactionContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link RunningQueriesRegistry}.
 */
@ExtendWith(MockitoExtension.class)
public class RunningQueriesRegistryTest extends BaseIgniteAbstractTest {
    private final RunningQueriesRegistry registry = new RunningQueriesRegistryImpl();

    @Mock
    private TxManager txManager;

    @Mock
    private TransactionInflights transactionInflights;

    @Mock
    private HybridTimestampTracker observableTimeTracker;

    @Test
    public void registerQuery() {
        NoOpTransaction tx = NoOpTransaction.readWrite("RW");
        QueryTransactionContext txContext = ExplicitTxContext.fromTx(tx);

        RunningQueryInfo queryInfo = registry.registerQuery("test", "SELECT 1;", txContext.explicitTx());

        assertThat(registry.queries(), hasSize(1));
        assertSame(queryInfo, registry.queries().iterator().next());

        assertThat(queryInfo.queryId(), notNullValue());
        assertThat(queryInfo.phase(), equalTo(QueryExecutionPhase.INITIALIZATION.name()));
        assertThat(queryInfo.startTime(), lessThanOrEqualTo(Instant.now()));
        assertThat(queryInfo.queryType(), nullValue());
        assertThat(queryInfo.sql(), equalTo("SELECT 1;"));
        assertThat(queryInfo.schema(), equalTo("test"));
        assertThat(queryInfo.parentId(), nullValue());
        assertThat(queryInfo.statementNumber(), nullValue());
        assertThat(queryInfo.transactionId(), equalTo(tx.id().toString()));

        registry.unregister(queryInfo.queryId());

        assertThat(registry.queries(), hasSize(0));
        assertThat(registry.openedCursorsCount(), is(0));
    }

    @Test
    public void registerScriptWithTx() {
        String schema = "test";

        QueryTransactionContext txContext = new QueryTransactionContextImpl(txManager, observableTimeTracker, null, transactionInflights);

        when(txManager.begin(any(), anyBoolean())).thenReturn(NoOpTransaction.readWrite("node1"));

        ScriptTransactionContext scriptTxContext = new ScriptTransactionContext(txContext, transactionInflights);

        RunningScriptInfoTracker scriptInfoTracker = registry.registerScript(schema, "SELECT 1;", scriptTxContext);

        assertThat(registry.queries(), hasSize(1));

        RunningQueryInfo scriptQueryInfo = registry.queries().iterator().next();

        // Verify initial script query info.
        {
            assertThat(scriptQueryInfo.queryId(), notNullValue());
            assertThat(scriptQueryInfo.phase(), equalTo(QueryExecutionPhase.INITIALIZATION.name()));
            assertThat(scriptQueryInfo.startTime(), lessThanOrEqualTo(Instant.now()));
            assertThat(scriptQueryInfo.queryType(), equalTo(SCRIPT_QUERY_TYPE));
            assertThat(scriptQueryInfo.sql(), equalTo("SELECT 1;"));
            assertThat(scriptQueryInfo.schema(), equalTo(schema));
            assertThat(scriptQueryInfo.parentId(), nullValue());
            assertThat(scriptQueryInfo.statementNumber(), nullValue());
            assertThat(scriptQueryInfo.transactionId(), nullValue());
        }

        // After initialization, the script must be moved to the execution phase.
        scriptInfoTracker.onInitializationComplete(2);
        assertThat(scriptQueryInfo.phase(), equalTo(QueryExecutionPhase.EXECUTION.name()));

        // Simulate transaction start within script.
        scriptTxContext.handleControlStatement(Mockito.mock(IgniteSqlStartTransaction.class));

        QueryTransactionWrapper txWrapper = scriptTxContext.explicitTx();
        assertThat(txWrapper, notNullValue());

        String expectedTxId = txWrapper.unwrap().id().toString();

        RunningQueryInfo statement1 = scriptInfoTracker.registerStatement("SELECT 1", SqlQueryType.QUERY);
        assertThat(registry.queries(), hasSize(2));

        RunningQueryInfo statement2 = scriptInfoTracker.registerStatement("SELECT 2", SqlQueryType.QUERY);
        assertThat(registry.queries(), hasSize(3));

        // Verify script statements info.
        {
            assertThat(statement1.queryId(), notNullValue());
            assertThat(statement2.queryId(), notNullValue());

            assertThat(statement1.queryId(), not(equalTo(statement2.queryId())));

            assertThat(statement1.phase(), equalTo(QueryExecutionPhase.INITIALIZATION.name()));
            assertThat(statement2.phase(), equalTo(QueryExecutionPhase.INITIALIZATION.name()));

            assertThat(statement1.startTime(), lessThanOrEqualTo(Instant.now()));
            assertThat(statement2.startTime(), lessThanOrEqualTo(Instant.now()));

            assertThat(statement1.queryType(), equalTo(SqlQueryType.QUERY.name()));
            assertThat(statement2.queryType(), equalTo(SqlQueryType.QUERY.name()));

            assertThat(statement1.sql(), equalTo("SELECT 1"));
            assertThat(statement2.sql(), equalTo("SELECT 2"));

            assertThat(statement1.schema(), equalTo(schema));
            assertThat(statement2.schema(), equalTo(schema));

            assertThat(statement1.parentId(), equalTo(scriptQueryInfo.queryId().toString()));
            assertThat(statement2.parentId(), equalTo(scriptQueryInfo.queryId().toString()));

            assertThat(statement1.statementNumber(), is(1));
            assertThat(statement2.statementNumber(), is(2));

            assertThat(statement1.transactionId(), equalTo(expectedTxId));
            assertThat(statement2.transactionId(), equalTo(expectedTxId));
        }

        // Simulate cursor registration.
        AsyncSqlCursor<?> cursor = Mockito.mock(AsyncSqlCursor.class);
        CompletableFuture<Void> closeFut = new CompletableFuture<>();
        when(cursor.onClose()).thenReturn(closeFut);

        registry.registerCursor(statement2, cursor);
        assertThat(registry.openedCursorsCount(), is(1));
        assertThat(registry.queries(), hasSize(3));

        // Closing the cursor must unregister the statement.
        closeFut.complete(null);
        assertThat(registry.openedCursorsCount(), is(0));
        assertThat(registry.queries(), hasSize(2));

        // When the last script statement unregisters itself,
        // the script query must be unregistered as well.
        registry.unregister(statement1.queryId());
        assertThat(registry.queries(), hasSize(0));
    }

    @Test
    public void registerScriptWithExplicitTx() {
        String schema = "test";

        NoOpTransaction tx = NoOpTransaction.readWrite("node1");
        QueryTransactionContext txContext = new QueryTransactionContextImpl(txManager, observableTimeTracker, tx, transactionInflights);

        ScriptTransactionContext scriptTxContext = new ScriptTransactionContext(txContext, transactionInflights);

        RunningScriptInfoTracker scriptInfoTracker = registry.registerScript(schema, "SELECT 1;", scriptTxContext);

        assertThat(registry.queries(), hasSize(1));

        scriptInfoTracker.onInitializationComplete(2);

        RunningQueryInfo scriptInfo = registry.queries().iterator().next();
        RunningQueryInfo stmt1 = scriptInfoTracker.registerStatement("SELECT 1", SqlQueryType.QUERY);
        RunningQueryInfo stmt2 = scriptInfoTracker.registerStatement("SELECT 2", SqlQueryType.QUERY);

        assertThat(scriptInfo.transactionId(), equalTo(tx.id().toString()));
        assertThat(stmt1.transactionId(), equalTo(tx.id().toString()));
        assertThat(stmt2.transactionId(), equalTo(tx.id().toString()));

        registry.unregister(stmt1.queryId());
        registry.unregister(stmt2.queryId());

        assertThat(registry.queries(), hasSize(0));
    }
}
