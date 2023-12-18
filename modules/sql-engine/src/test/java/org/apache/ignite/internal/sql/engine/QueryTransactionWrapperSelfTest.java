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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransaction;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapperImpl;
import org.apache.ignite.internal.sql.engine.tx.ScriptTransactionContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for class {@link QueryTransactionWrapperImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class QueryTransactionWrapperSelfTest extends BaseIgniteAbstractTest {
    @Mock
    private IgniteTransactions transactions;

    @Test
    public void testImplicitTransactionAttributes() {
        when(transactions.begin(any())).thenAnswer(
                inv -> {
                    boolean readOnly = inv.getArgument(0, TransactionOptions.class).readOnly();

                    return readOnly ? NoOpTransaction.readOnly("test-ro") : NoOpTransaction.readWrite("test-rw");
                }
        );

        QueryTransactionContext transactionHandler = new QueryTransactionContext(transactions, null);
        QueryTransactionWrapper transactionWrapper = transactionHandler.getOrStartImplicit(SqlQueryType.DML);

        assertThat(transactionWrapper.unwrap().isReadOnly(), equalTo(false));

        for (SqlQueryType type : EnumSet.complementOf(EnumSet.of(SqlQueryType.DML))) {
            transactionWrapper = transactionHandler.getOrStartImplicit(type);
            assertThat(transactionWrapper.unwrap().isReadOnly(), equalTo(true));
        }

        verify(transactions, times(SqlQueryType.values().length)).begin(any());
        verifyNoMoreInteractions(transactions);
    }

    @Test
    public void commitImplicitTxNotAffectExternalTransaction() {
        NoOpTransaction externalTx = new NoOpTransaction("test");

        QueryTransactionWrapperImpl wrapper = new QueryTransactionWrapperImpl(externalTx, false);
        wrapper.commitImplicit();
        assertFalse(externalTx.commitFuture().isDone());
    }

    @Test
    public void testCommitImplicit() {
        NoOpTransaction tx = new NoOpTransaction("test");
        QueryTransactionWrapperImpl wrapper = new QueryTransactionWrapperImpl(tx, true);

        wrapper.commitImplicit();

        assertThat(tx.commitFuture().isDone(), equalTo(true));
        assertThat(tx.rollbackFuture().isDone(), equalTo(false));
    }

    @Test
    public void testRollbackImplicit() {
        NoOpTransaction tx = new NoOpTransaction("test");
        QueryTransactionWrapperImpl wrapper = new QueryTransactionWrapperImpl(tx, true);

        wrapper.rollback(null);

        assertThat(tx.rollbackFuture().isDone(), equalTo(true));
        assertThat(tx.commitFuture().isDone(), equalTo(false));
    }

    @Test
    public void throwsExceptionForDdlWithExternalTransaction() {
        QueryTransactionContext txCtx = new QueryTransactionContext(transactions, new NoOpTransaction("test"));

        //noinspection ThrowableNotThrown
        assertThrowsSqlException(Sql.RUNTIME_ERR, "DDL doesn't support transactions.",
                () -> txCtx.getOrStartImplicit(SqlQueryType.DDL));

        verifyNoInteractions(transactions);
    }

    @Test
    public void throwsExceptionForDmlWithReadOnlyExternalTransaction() {
        QueryTransactionContext txCtx = new QueryTransactionContext(transactions, new NoOpTransaction("test"));

        //noinspection ThrowableNotThrown
        assertThrowsSqlException(Sql.RUNTIME_ERR, "DML query cannot be started by using read only transactions.",
                () -> txCtx.getOrStartImplicit(SqlQueryType.DML));

        verifyNoInteractions(transactions);
    }

    @Test
    public void throwsExceptionForTxControlStatementInsideExternalTransaction() {
        ScriptTransactionContext txCtx = new ScriptTransactionContext(
                new QueryTransactionContext(transactions, new NoOpTransaction("test")));

        assertThrowsExactly(TxControlInsideExternalTxNotSupportedException.class, () -> txCtx.handleControlStatement(null));
    }

    @Test
    public void throwsExceptionForNestedScriptTransaction() {
        ScriptTransactionContext txCtx = new ScriptTransactionContext(new QueryTransactionContext(transactions, null));
        IgniteSqlStartTransaction txStartStmt = mock(IgniteSqlStartTransaction.class);

        when(transactions.begin(any())).thenReturn(new NoOpTransaction("test"));

        txCtx.handleControlStatement(txStartStmt);

        //noinspection ThrowableNotThrown
        assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "Nested transactions are not supported.",
                () -> txCtx.handleControlStatement(txStartStmt)
        );
    }
}
