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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapperImpl;
import org.jetbrains.annotations.Nullable;

/** Context that always creates implicit transaction. */
public class ImplicitTxContext implements QueryTransactionContext {
    private final HybridTimestampTracker observableTimeTracker = HybridTimestampTracker.atomicTracker(null);

    public static ImplicitTxContext create() {
        return new ImplicitTxContext();
    }

    private ImplicitTxContext() { }

    @Override
    public QueryTransactionWrapper getOrStartSqlManaged(boolean readOnly, boolean implicit) {
        return new QueryTransactionWrapperImpl(new NoOpTransaction("dummy", false), true, NoOpTransactionalOperationTracker.INSTANCE);
    }

    @Override
    public @Nullable QueryTransactionWrapper explicitTx() {
        return null;
    }

    @Override
    public void updateObservableTime(HybridTimestamp time) {
        observableTimeTracker.update(time);
    }

    public @Nullable HybridTimestamp observableTime() {
        return observableTimeTracker.get();
    }
}
