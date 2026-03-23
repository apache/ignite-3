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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCommitTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransactionMode;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContextImpl;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapperImpl;
import org.apache.ignite.internal.sql.engine.tx.ScriptTransactionContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.lang.ErrorGroups.Sql;
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
    private HybridTimestampTracker observableTimeTracker;

    @Mock
    private TxManager txManager;

    @Mock
    private TransactionInflights transactionInflights;

    @Test
    public void testImplicitTransactionAttributes() {
        prepareTransactionsMocks();

        when(transactionInflights.track(any())).thenReturn(true);
        when(transactionInflights.addScanInflight(any())).thenReturn(true);

        QueryTransactionContext transactionHandler = new QueryTransactionContextImpl(txManager, observableTimeTracker, null,
                new InflightTransactionalOperationTracker(transactionInflights, txManager));
        QueryTransactionWrapper transactionWrapper = transactionHandler.getOrStartSqlManaged(false, false);

        assertThat(transactionWrapper.unwrap().isReadOnly(), equalTo(false));

        transactionWrapper = transactionHandler.getOrStartSqlManaged(true, false);
        assertThat(transactionWrapper.unwrap().isReadOnly(), equalTo(true));
    }

    @Test
    public void commitImplicitTxNotAffectExternalTransaction() {
        NoOpTransaction externalTx = new NoOpTransaction("test", false);

        QueryTransactionWrapperImpl wrapper = new QueryTransactionWrapperImpl(externalTx, false,
                new InflightTransactionalOperationTracker(transactionInflights, txManager));
        wrapper.finalise();
        assertFalse(externalTx.commitFuture().isDone());
    }

    @Test
    public void testCommitImplicit() {
        NoOpTransaction tx = new NoOpTransaction("test", false);
        QueryTransactionWrapperImpl wrapper = new QueryTransactionWrapperImpl(tx, true,
                new InflightTransactionalOperationTracker(transactionInflights, txManager));

        wrapper.finalise();

        assertThat(tx.commitFuture().isDone(), equalTo(true));
        assertThat(tx.rollbackFuture().isDone(), equalTo(false));
    }

    @Test
    public void testRollbackImplicit() {
        NoOpTransaction tx = new NoOpTransaction("test", false);
        QueryTransactionWrapperImpl wrapper = new QueryTransactionWrapperImpl(tx, true,
                new InflightTransactionalOperationTracker(transactionInflights, txManager));

        wrapper.finalise(new RuntimeException("Test exception"));

        assertThat(tx.rollbackFuture().isDone(), equalTo(true));
        assertThat(tx.commitFuture().isDone(), equalTo(false));
    }

    @Test
    public void throwsExceptionForTxControlStatementInsideExternalTransaction() {
        var operationTracker = new InflightTransactionalOperationTracker(transactionInflights, txManager);
        ScriptTransactionContext txCtx = new ScriptTransactionContext(
                new QueryTransactionContextImpl(txManager, observableTimeTracker, new NoOpTransaction("test", false),
                        operationTracker),
                operationTracker
        );

        assertThrowsExactly(TxControlInsideExternalTxNotSupportedException.class, () -> txCtx.handleControlStatement(null));
    }

    @Test
    public void throwsExceptionForNestedScriptTransaction() {
        var operationTracker = new InflightTransactionalOperationTracker(transactionInflights, txManager);
        ScriptTransactionContext txCtx = new ScriptTransactionContext(
                new QueryTransactionContextImpl(txManager, observableTimeTracker, null, operationTracker),
                operationTracker
        );
        IgniteSqlStartTransaction txStartStmt = mock(IgniteSqlStartTransaction.class);

        when(txManager.beginExplicit(any(), anyBoolean(), any())).thenAnswer(inv -> {
            boolean implicit = inv.getArgument(1, Boolean.class);

            return NoOpTransaction.readWrite("test", implicit);
        });

        when(transactionInflights.track(any())).thenReturn(true);
        txCtx.handleControlStatement(txStartStmt);

        assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "Nested transactions are not supported.",
                () -> txCtx.handleControlStatement(txStartStmt)
        );
    }

    @Test
    public void testQueryTransactionWrapperTxInflightsInteraction() {
        Set<UUID> inflights = new HashSet<>();

        prepareTxInflightsMocks(inflights);

        prepareTransactionsMocks();

        QueryTransactionContext implicitDmlTxCtx = new QueryTransactionContextImpl(txManager, observableTimeTracker, null,
                new InflightTransactionalOperationTracker(transactionInflights, txManager));
        implicitDmlTxCtx.getOrStartSqlManaged(false, false);
        // Check that RW txns are tracked.
        log.info("inflights={}", inflights);
        assertEquals(1, inflights.size());

        QueryTransactionContext implicitQueryTxCtx = new QueryTransactionContextImpl(txManager, observableTimeTracker, null,
                new InflightTransactionalOperationTracker(transactionInflights, txManager));
        QueryTransactionWrapper implicitQueryTxWrapper = implicitQueryTxCtx.getOrStartSqlManaged(true, false);
        assertTrue(inflights.contains(implicitQueryTxWrapper.unwrap().id()));
        implicitQueryTxWrapper.finalise().join();
        assertEquals(1, inflights.size());

        NoOpTransaction rwTx = NoOpTransaction.readWrite("test-rw", false);
        QueryTransactionContext explicitRwTxCtx = new QueryTransactionContextImpl(txManager, observableTimeTracker, rwTx,
                new InflightTransactionalOperationTracker(transactionInflights, txManager));
        QueryTransactionWrapper explicitRwTxWrapper = explicitRwTxCtx.getOrStartSqlManaged(true, false);
        assertTrue(inflights.contains(explicitRwTxWrapper.unwrap().id()));
        // Check that RW txns are tracked.
        assertEquals(2, inflights.size());

        NoOpTransaction roTx = NoOpTransaction.readOnly("test-ro", false);
        QueryTransactionContext explicitRoTxCtx = new QueryTransactionContextImpl(txManager, observableTimeTracker, roTx,
                new InflightTransactionalOperationTracker(transactionInflights, txManager));
        QueryTransactionWrapper explicitRoTxWrapper = explicitRoTxCtx.getOrStartSqlManaged(true, false);
        assertTrue(inflights.contains(explicitRoTxWrapper.unwrap().id()));
        explicitRoTxWrapper.finalise();
        assertEquals(2, inflights.size());
    }

    @Test
    public void testScriptTransactionWrapperTxInflightsInteraction() {
        Set<UUID> inflights = new HashSet<>();

        prepareTxInflightsMocks(inflights);

        prepareTransactionsMocks();

        var operationTracker = new InflightTransactionalOperationTracker(transactionInflights, txManager);
        QueryTransactionContext txCtx = new QueryTransactionContextImpl(txManager, observableTimeTracker, null,
                operationTracker);
        ScriptTransactionContext scriptRwTxCtx = new ScriptTransactionContext(txCtx, operationTracker);

        IgniteSqlStartTransaction sqlStartRwTx = mock(IgniteSqlStartTransaction.class);
        when(sqlStartRwTx.getMode()).thenAnswer(inv -> IgniteSqlStartTransactionMode.READ_WRITE);

        scriptRwTxCtx.handleControlStatement(sqlStartRwTx);
        assertFalse(inflights.isEmpty());

        ScriptTransactionContext scriptRoTxCtx = new ScriptTransactionContext(txCtx, operationTracker);
        IgniteSqlStartTransaction sqlStartRoTx = mock(IgniteSqlStartTransaction.class);
        when(sqlStartRoTx.getMode()).thenAnswer(inv -> IgniteSqlStartTransactionMode.READ_ONLY);

        scriptRoTxCtx.handleControlStatement(sqlStartRoTx);
        assertEquals(2, inflights.size());

        QueryTransactionWrapper wrapper = scriptRoTxCtx.getOrStartSqlManaged(true, false);
        assertEquals(2, inflights.size());

        // ScriptTransactionWrapperImpl.commitImplicit is noop.
        wrapper.finalise();
        assertEquals(2, inflights.size());

        IgniteSqlCommitTransaction sqlCommitTx = mock(IgniteSqlCommitTransaction.class);
        scriptRoTxCtx.handleControlStatement(sqlCommitTx);
        assertEquals(1, inflights.size());
    }

    private void prepareTransactionsMocks() {
        when(txManager.beginExplicit(any(), anyBoolean(), any())).thenAnswer(
                inv -> {
                    boolean readOnly = inv.getArgument(1, Boolean.class);

                    return readOnly ? NoOpTransaction.readOnly("test-ro", false) : NoOpTransaction.readWrite("test-rw", false);
                }
        );
    }

    private void prepareTxInflightsMocks(Set<UUID> inflights) {
        when(transactionInflights.addScanInflight(any())).thenAnswer(inv -> inflights.add(inv.getArgument(0)));

        doAnswer(inv -> inflights.remove(inv.getArgument(0))).when(transactionInflights).removeInflight(any());

        when(transactionInflights.track(any())).thenAnswer(inv -> inflights.add(inv.getArgument(0)));
    }
}
