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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ReadOnlyTransactionImplTest extends BaseIgniteAbstractTest {
    @Mock
    private TxManagerImpl txManager;

    @Test
    void effectiveSchemaTimestampIsReadTimestamp() {
        HybridTimestamp readTimestamp = new HybridClockImpl().now();
        UUID txId = TestTransactionIds.TRANSACTION_ID_GENERATOR.transactionIdFor(readTimestamp);

        var tx = new ReadOnlyTransactionImpl(
                txManager,
                HybridTimestampTracker.atomicTracker(null),
                txId,
                new UUID(1, 2),
                10_000,
                readTimestamp,
                new CompletableFuture<>(),
                options.killClosure());

        assertThat(tx.schemaTimestamp(), is(readTimestamp));
    }
}
