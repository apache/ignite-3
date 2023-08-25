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

import static org.apache.ignite.internal.sql.engine.SqlQueryProcessor.wrapTxOrStartImplicit;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
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
public class QueryTransactionWrapperSelfTest extends BaseIgniteAbstractTest {
    @Mock
    private IgniteTransactions transactions;

    @Test
    public void throwsExceptionForDdlWithExternalTransaction() {
        //noinspection ThrowableNotThrown
        assertThrowsSqlException(ErrorGroups.Sql.STMT_VALIDATION_ERR,
                () -> wrapTxOrStartImplicit(SqlQueryType.DDL, transactions, new NoOpTransaction("test")));
        verifyNoInteractions(transactions);
    }

    @Test
    public void testImplicitTransactionAttributes() {
        when(transactions.begin(any())).thenAnswer(
                inv -> {
                    boolean readOnly = inv.getArgument(0, TransactionOptions.class).readOnly();

                    return readOnly ? NoOpTransaction.readOnly("test-ro") : NoOpTransaction.readWrite("test-rw");
                }
        );

        assertThat(wrapTxOrStartImplicit(SqlQueryType.QUERY, transactions, null).unwrap().isReadOnly(), equalTo(true));
        assertThat(wrapTxOrStartImplicit(SqlQueryType.DML, transactions, null).unwrap().isReadOnly(), equalTo(false));

        verify(transactions, times(2)).begin(any());
        verifyNoMoreInteractions(transactions);
    }

    @Test
    public void commitAndRollbackNotAffectExternalTransaction() {
        NoOpTransaction externalTx = new NoOpTransaction("test");

        QueryTransactionWrapper wrapper = wrapTxOrStartImplicit(SqlQueryType.QUERY, transactions, externalTx);
        wrapper.commitImplicit();
        assertFalse(externalTx.commitFuture().isDone());

        wrapper = wrapTxOrStartImplicit(SqlQueryType.QUERY, transactions, externalTx);
        wrapper.rollbackImplicit();
        assertFalse(externalTx.commitFuture().isDone());

        verifyNoInteractions(transactions);
    }

    @Test
    public void testCommitImplicit() {
        QueryTransactionWrapper wrapper = prepareImplicitTx();
        NoOpTransaction tx = (NoOpTransaction) wrapper.unwrap();

        wrapper.commitImplicit();

        assertThat(tx.commitFuture().isDone(), equalTo(true));
        assertThat(tx.rollbackFuture().isDone(), equalTo(false));
    }

    @Test
    public void testRollbackImplicit() {
        QueryTransactionWrapper wrapper = prepareImplicitTx();
        NoOpTransaction tx = (NoOpTransaction) wrapper.unwrap();

        wrapper.rollbackImplicit();

        assertThat(tx.rollbackFuture().isDone(), equalTo(true));
        assertThat(tx.commitFuture().isDone(), equalTo(false));
    }

    private QueryTransactionWrapper prepareImplicitTx() {
        when(transactions.begin(any())).thenReturn(new NoOpTransaction("test"));

        QueryTransactionWrapper wrapper = wrapTxOrStartImplicit(SqlQueryType.QUERY, transactions, null);

        assertThat(wrapper.unwrap(), instanceOf(NoOpTransaction.class));
        NoOpTransaction tx = (NoOpTransaction) wrapper.unwrap();

        assertFalse(tx.rollbackFuture().isDone());
        assertFalse(tx.commitFuture().isDone());

        return wrapper;
    }
}

