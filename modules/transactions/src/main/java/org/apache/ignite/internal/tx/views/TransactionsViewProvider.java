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

package org.apache.ignite.internal.tx.views;

import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * {@code TRANSACTIONS} system view provider.
 */
public class TransactionsViewProvider {
    public static final String READ_ONLY = "READ_ONLY";

    public static final String READ_WRITE = "READ_WRITE";

    private volatile Iterable<TxInfo> dataSource;

    /** Initializes provider with data sources. */
    public void init(
            UUID localNodeId,
            Map<UUID, TxStateMeta> rwTxStates
    ) {
        this.dataSource = new TxInfoDataSource(
                localNodeId,
                rwTxStates
        );
    }

    /** Returns a {@code TRANSACTIONS} system view. */
    public SystemView<?> get() {
        Publisher<TxInfo> dataProvider = SubscriptionUtils.fromIterable(
                () -> {
                    Iterable<TxInfo> dataSource0 = dataSource;

                    return dataSource0 == null
                            ? Collections.emptyIterator()
                            : dataSource0.iterator();
                }
        );

        NativeType stringType = stringOf(64);
        NativeType timestampType = NativeTypes.timestamp(NativeTypes.MAX_TIME_PRECISION);

        return SystemViews.<TxInfo>nodeViewBuilder()
                .name("TRANSACTIONS")
                .nodeNameColumnAlias("COORDINATOR_NODE_ID")
                .<String>addColumn("STATE", stringType, tx -> tx.state)
                .<String>addColumn("ID", stringType, tx -> tx.id)
                .<Instant>addColumn("START_TIME", timestampType, tx -> tx.startTime)
                .<String>addColumn("TYPE", stringType, tx -> tx.type)
                .<String>addColumn("PRIORITY", stringType, tx -> tx.priority)
                .dataProvider(dataProvider)
                .build();
    }

    static class TxInfoDataSource implements Iterable<TxInfo> {
        private final UUID localNodeId;

        private final Map<UUID, TxStateMeta> rwTxStates;

        TxInfoDataSource(UUID localNodeId, Map<UUID, TxStateMeta> rwTxStates) {
            this.localNodeId = localNodeId;
            this.rwTxStates = rwTxStates;
        }

        @Override
        public Iterator<TxInfo> iterator() {
            return rwTxStates.entrySet().stream()
                            .filter(e -> localNodeId.equals(e.getValue().txCoordinatorId())
                                    && e.getValue().tx() != null && !e.getValue().tx().isFinishingOrFinished())
                            .map(e -> new TxInfo(e.getKey(), e.getValue()))
                            .iterator();
        }
    }

    static class TxInfo {
        private final String id;
        private final @Nullable String state;
        private final Instant startTime;
        private final String type;
        private final String priority;

        TxInfo(UUID id, TxStateMeta txStateMeta) {
            InternalTransaction tx = txStateMeta.tx();

            assert tx != null;

            this.id = id.toString();
            this.state = txStateMeta.txState().name();
            this.startTime = Instant.ofEpochMilli(TransactionIds.beginTimestamp(id).getPhysical());
            this.type = tx.isReadOnly() ? READ_ONLY : READ_WRITE;
            this.priority = TransactionIds.priority(id).name();
        }
    }
}
