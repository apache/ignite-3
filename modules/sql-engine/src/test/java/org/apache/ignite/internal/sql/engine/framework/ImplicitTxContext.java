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

package org.apache.ignite.internal.sql.engine.framework;

import java.util.UUID;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapperImpl;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.jetbrains.annotations.Nullable;

/** Context that always creates implicit transaction. */
public class ImplicitTxContext implements QueryTransactionContext {
    private static final HybridClock CLOCK = new HybridClockImpl();

    private static final TransactionInflights TX_INFLIGHTS = new TransactionInflights(
            new TestPlacementDriver("dummy", UUID.randomUUID()),
            new ClockServiceImpl(CLOCK, new ClockWaiter("dummy", CLOCK), () -> 1L)
    );

    public static final QueryTransactionContext INSTANCE = new ImplicitTxContext();

    private final HybridTimestampTracker observableTimeTracker = new HybridTimestampTracker();

    private ImplicitTxContext() { }

    @Override
    public QueryTransactionWrapper getOrStartImplicit(boolean readOnly) {
        return new QueryTransactionWrapperImpl(new NoOpTransaction("dummy"), true, TX_INFLIGHTS);
    }

    @Override
    public @Nullable QueryTransactionWrapper explicitTx() {
        return null;
    }

    @Override
    public void updateObservableTime(HybridTimestamp time) {
        observableTimeTracker.update(time);
    }
}
