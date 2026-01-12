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

import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import java.util.stream.Stream;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxState;
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

    private volatile TxInfoDataSource dataSource;

    /** Initializes provider with data sources. */
    public void init(UUID localNodeId, Map<UUID, TxStateMeta> txStates) {
        this.dataSource = new TxInfoDataSource(localNodeId, txStates);
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
                .<String>addColumn("TRANSACTION_STATE", stringType, tx -> tx.state)
                .<String>addColumn("TRANSACTION_ID", stringType, tx -> tx.id.toString())
                .<Instant>addColumn("TRANSACTION_START_TIME", timestampType, tx -> tx.startTime)
                .<String>addColumn("TRANSACTION_TYPE", stringType, tx -> tx.type)
                .<String>addColumn("TRANSACTION_PRIORITY", stringType, tx -> tx.priority)
                .<String>addColumn("TRANSACTION_LABEL", stringType, tx -> {
                    TxStateMeta meta = dataSource.txStates.get(tx.id);
                    return meta != null ? meta.txLabel() : null;
                })
                // TODO https://issues.apache.org/jira/browse/IGNITE-24589: Next columns are deprecated and should be removed.
                //  They are kept for compatibility with 3.0 version, to allow columns being found by their old names.
                .<String>addColumn("STATE", stringType, tx -> tx.state)
                .<String>addColumn("ID", stringType, tx -> tx.id.toString())
                .<Instant>addColumn("START_TIME", timestampType, tx -> tx.startTime)
                .<String>addColumn("TYPE", stringType, tx -> tx.type)
                .<String>addColumn("PRIORITY", stringType, tx -> tx.priority)
                // End of legacy columns list. New columns must be added below this line.
                .dataProvider(dataProvider)
                .build();
    }

    static class TxInfoDataSource implements Iterable<TxInfo> {
        private final UUID localNodeId;
        private final Map<UUID, TxStateMeta> txStates;

        TxInfoDataSource(UUID localNodeId, Map<UUID, TxStateMeta> txStates) {
            this.localNodeId = localNodeId;
            this.txStates = txStates;
        }

        @Override
        public Iterator<TxInfo> iterator() {
            return txStates.entrySet().stream()
                    .flatMap(entry -> {
                        UUID id = entry.getKey();
                        TxStateMeta txStateMeta = entry.getValue();
                        UUID coordinatorId = txStateMeta.txCoordinatorId();
                        InternalTransaction tx = txStateMeta.tx();

                        if (!Objects.equals(localNodeId, coordinatorId)) {
                            return Stream.empty();
                        }

                        // Currently the read-only transaction status does not change and it is always in the PENDING state.
                        if ((tx != null && tx.isReadOnly() && tx.isFinishingOrFinished()) || isFinalState(txStateMeta.txState())) {
                            return Stream.empty();
                        }

                        return Stream.of(new TxInfo(id, txStateMeta.txState(), deriveTransactionType(tx)));
                    })
                    .iterator();
        }

        private static String deriveTransactionType(@Nullable InternalTransaction tx) {
            // The transaction tx can be null under some circumstances,
            // even though this node is a transaction coordinator.
            if (tx != null) {
                return tx.isReadOnly() ? READ_ONLY : READ_WRITE;
            } else {
                return "n/a";
            }
        }
    }

    static class TxInfo {
        private final UUID id;
        private final String state;
        private final Instant startTime;
        private final String type;
        private final String priority;

        TxInfo(UUID txId, TxState txState, String type) {
            this.id = txId;
            this.state = txState.name();
            this.startTime = Instant.ofEpochMilli(TransactionIds.beginTimestamp(txId).getPhysical());
            this.type = type;
            this.priority = TransactionIds.priority(txId).name();
        }
    }
}
