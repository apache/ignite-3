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

import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.internal.util.TransformingIterator;
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
            Collection<UUID> roTxIds,
            Map<UUID, TxStateMeta> rwTxStates
    ) {
        this.dataSource = new TxInfoDataSource(
                localNodeId,
                roTxIds,
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

        private final Iterable<UUID> roTxIds;

        private final Map<UUID, TxStateMeta> rwTxStates;

        TxInfoDataSource(UUID localNodeId, Iterable<UUID> roTxIds, Map<UUID, TxStateMeta> rwTxStates) {
            this.localNodeId = localNodeId;
            this.roTxIds = roTxIds;
            this.rwTxStates = rwTxStates;
        }

        @Override
        public Iterator<TxInfo> iterator() {
            return CollectionUtils.concat(
                    new TransformingIterator<>(roTxIds.iterator(), TxInfo::readOnly),
                    rwTxStates.entrySet().stream()
                            .filter(e -> localNodeId.equals(e.getValue().txCoordinatorId())
                                    && e.getValue().tx() != null && !e.getValue().tx().isReadOnly()
                                    && !isFinalState(e.getValue().txState()))
                            .map(e -> TxInfo.readWrite(e.getKey(), e.getValue().txState()))
                            .iterator()
            );
        }
    }

    static class TxInfo {
        private final String id;
        private final @Nullable String state;
        private final Instant startTime;
        private final String type;
        private final String priority;

        static TxInfo readOnly(UUID id) {
            return new TxInfo(id, PENDING, true);
        }

        static TxInfo readWrite(UUID id, TxState txState) {
            return new TxInfo(id, txState, false);
        }

        private TxInfo(UUID id, @Nullable TxState state, boolean readOnly) {
            this.id = id.toString();
            this.state = state == null ? null : state.name();
            this.startTime = Instant.ofEpochMilli(TransactionIds.beginTimestamp(id).getPhysical());
            this.type = readOnly ? READ_ONLY : READ_WRITE;
            this.priority = TransactionIds.priority(id).name();
        }
    }
}
