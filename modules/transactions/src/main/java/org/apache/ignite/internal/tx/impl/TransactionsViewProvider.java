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

import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * {@code TRANSACTIONS} system view provider.
 */
class TransactionsViewProvider {
    private final Map<UUID, InternalTransaction> transactions;

    TransactionsViewProvider(Map<UUID, InternalTransaction> transactions) {
        this.transactions = transactions;
    }

    /** Returns a {@code TRANSACTIONS} system view. */
    SystemView<?> get() {
        NativeType stringType = stringOf(64);
        NativeType timestampType = NativeTypes.timestamp(NativeTypes.MAX_TIME_PRECISION);

        return SystemViews.<InternalTransaction>nodeViewBuilder()
                .name("TRANSACTIONS")
                .nodeNameColumnAlias("COORDINATOR_NODE")
                .<String>addColumn("STATE", stringType, tx -> deriveState(tx.state()))
                .<String>addColumn("ID", stringType, tx -> tx.id().toString())
                .<Instant>addColumn("START_TIME", timestampType, tx -> Instant.ofEpochMilli(tx.startTimestamp().getPhysical()))
                .<String>addColumn("TYPE", stringType, tx -> tx.isReadOnly() ? "READ ONLY" : "READ WRITE")
                .<String>addColumn("PRIORITY", stringType, tx -> TransactionIds.priority(tx.id()).name())
                .dataProvider(SubscriptionUtils.fromIterable(transactions.values()))
                .build();
    }

    private static @Nullable String deriveState(@Nullable TxState state) {
        return state == null ? null : state.name();
    }
}
