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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class TransactionLogUtilsTest extends BaseIgniteAbstractTest {
    @Test
    void formatsWrappedWhenDefaultOverloadUsed() {
        UUID txId = UUID.randomUUID();

        TxManager txManager = mock(TxManager.class);
        when(txManager.stateMeta(txId)).thenReturn(TxStateMeta.builder(PENDING).txLabel("lbl").build());

        assertEquals("[txId=" + txId + ", txLabel=lbl]", TransactionLogUtils.formatTxInfo(txId, txManager));
    }

    @Test
    void formatsUnwrappedWhenWrappedFalse() {
        UUID txId = UUID.randomUUID();

        TxManager txManager = mock(TxManager.class);
        when(txManager.stateMeta(txId)).thenReturn(TxStateMeta.builder(PENDING).txLabel("lbl").build());

        assertEquals("txId=" + txId + ", txLabel=lbl", TransactionLogUtils.formatTxInfo(txId, txManager, false));
    }

    @Test
    void emptyLabelIsNotPrintedWhenUnwrapped() {
        UUID txId = UUID.randomUUID();

        TxManager txManager = mock(TxManager.class);
        when(txManager.stateMeta(txId)).thenReturn(TxStateMeta.builder(PENDING).txLabel("").build());

        assertEquals("txId=" + txId, TransactionLogUtils.formatTxInfo(txId, txManager, false));
    }

    @Test
    void nullTxIdIsFormattedWithOrWithoutWrapping() {
        TxManager txManager = mock(TxManager.class);

        assertEquals("[txId=null]", TransactionLogUtils.formatTxInfo(null, txManager, true));
        assertEquals("txId=null", TransactionLogUtils.formatTxInfo(null, txManager, false));
    }
}
