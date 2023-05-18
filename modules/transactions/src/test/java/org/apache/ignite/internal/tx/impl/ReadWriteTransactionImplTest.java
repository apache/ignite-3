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

package org.apache.ignite.internal.tx.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ReadWriteTransactionImplTest {
    @Mock
    private TxManager txManager;

    private final HybridClock clock = new HybridClockImpl();

    @Test
    void effectiveSchemaTimestampIsBeginTimestamp() {
        HybridTimestamp beginTs = clock.now();
        UUID txId = TestTransactionIds.TRANSACTION_ID_GENERATOR.transactionIdFor(beginTs);

        var tx = new ReadWriteTransactionImpl(txManager, txId);

        assertThat(tx.startTimestamp(), is(beginTs));
    }
}
