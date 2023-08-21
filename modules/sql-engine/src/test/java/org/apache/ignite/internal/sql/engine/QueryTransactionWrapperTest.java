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
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for class {@link QueryTransactionWrapper}.
 */
@ExtendWith(MockitoExtension.class)
public class QueryTransactionWrapperTest {
    @Mock
    private IgniteTransactions transactions;

    @Test
    public void throwsExceptionForDdlWithExternalTransaction() {
        QueryTransactionWrapper wrapper = new QueryTransactionWrapper(transactions, new NoOpTransaction("test"));

        //noinspection ThrowableNotThrown
        assertThrowsSqlException(ErrorGroups.Sql.STMT_VALIDATION_ERR, () -> wrapper.beginTxIfNeeded(SqlQueryType.DDL));
        verifyNoInteractions(transactions);
    }

    @Test
    public void noImplicitTransactionForDdl() {
        QueryTransactionWrapper wrapper = new QueryTransactionWrapper(transactions, null);

        assertNull(wrapper.beginTxIfNeeded(SqlQueryType.DDL));
        assertNull(wrapper.transaction());

        verifyNoInteractions(transactions);
    }

    @Test
    public void noImplicitTransactionForExplain() {
        QueryTransactionWrapper wrapper = new QueryTransactionWrapper(transactions, null);

        assertNull(wrapper.beginTxIfNeeded(SqlQueryType.EXPLAIN));
        assertNull(wrapper.transaction());

        verifyNoInteractions(transactions);
    }

    @Test
    public void testImplicitTransactionAttributes() {
        when(transactions.begin(any())).thenAnswer(
                inv -> new NoOpTransaction("test", inv.getArgument(0, TransactionOptions.class).readOnly())
        );

        QueryTransactionWrapper wrapper = new QueryTransactionWrapper(transactions, null);

        assertNotNull(wrapper.beginTxIfNeeded(SqlQueryType.QUERY));
        assertTrue(wrapper.transaction().isReadOnly());

        assertNotNull(wrapper.beginTxIfNeeded(SqlQueryType.DML));
        assertFalse(wrapper.transaction().isReadOnly());

        verify(transactions, times(2)).begin(any());
        verifyNoMoreInteractions(transactions);
    }

    @Test
    public void commitAndRollbackNotAffectExternalTransaction() {
        NoOpTransaction externalTx = new NoOpTransaction("test");
        QueryTransactionWrapper wrapper = new QueryTransactionWrapper(transactions, externalTx);

        wrapper.beginTxIfNeeded(SqlQueryType.QUERY);
        wrapper.commitImplicit();
        assertFalse(externalTx.commitFuture().isDone());

        wrapper.beginTxIfNeeded(SqlQueryType.QUERY);
        wrapper.rollbackImplicit();
        assertFalse(externalTx.commitFuture().isDone());

        verifyNoInteractions(transactions);
    }

    @Test
    public void testCommitImplicit() {
        when(transactions.begin(any())).thenReturn(new NoOpTransaction("test"));

        QueryTransactionWrapper wrapper = new QueryTransactionWrapper(transactions, null);

        wrapper.beginTxIfNeeded(SqlQueryType.QUERY);

        assertThat(wrapper.transaction(), instanceOf(NoOpTransaction.class));
        NoOpTransaction tx = (NoOpTransaction) wrapper.transaction();

        assertFalse(tx.commitFuture().isDone());

        wrapper.commitImplicit();

        assertTrue(tx.commitFuture().isDone());
        assertFalse(tx.rollbackFuture().isDone());
    }

    @Test
    public void testRollbackImplicit() {
        when(transactions.begin(any())).thenReturn(new NoOpTransaction("test"));

        QueryTransactionWrapper wrapper = new QueryTransactionWrapper(transactions, null);

        wrapper.beginTxIfNeeded(SqlQueryType.QUERY);

        assertThat(wrapper.transaction(), instanceOf(NoOpTransaction.class));
        NoOpTransaction tx = (NoOpTransaction) wrapper.transaction();

        assertFalse(tx.rollbackFuture().isDone());

        wrapper.rollbackImplicit();

        assertTrue(tx.rollbackFuture().isDone());
        assertFalse(tx.commitFuture().isDone());
    }
}
