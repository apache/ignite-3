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

import it.unimi.dsi.fastutil.Pair;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxState;
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

    private final Iterable<UUID> roTxDatasource;

    private final Iterable<Pair<UUID, TxState>> rwTxDatasource;

    TransactionsViewProvider(
            Iterable<UUID> roTxDatasource,
            Iterable<Pair<UUID, TxState>> rwTxDatasource
    ) {
        this.roTxDatasource = roTxDatasource;
        this.rwTxDatasource = rwTxDatasource;
    }

    /** Returns a {@code TRANSACTIONS} system view. */
    SystemView<?> get() {
        NativeType stringType = stringOf(64);
        NativeType timestampType = NativeTypes.timestamp(NativeTypes.MAX_TIME_PRECISION);

        Publisher<TxInfo> publisher = SubscriptionUtils.fromIterable(CollectionUtils.concat(
                () -> new TransformingIterator<>(roTxDatasource.iterator(), TxInfo::forReadOnly),
                () -> new TransformingIterator<>(rwTxDatasource.iterator(), pair -> TxInfo.forReadWrite(pair.first(), pair.second()))
        ));

        return SystemViews.<TxInfo>nodeViewBuilder()
                .name("TRANSACTIONS")
                .nodeNameColumnAlias("COORDINATOR_NODE_ID")
                .<String>addColumn("STATE", stringType, tx -> tx.state)
                .<String>addColumn("ID", stringType, tx -> tx.id)
                .<Instant>addColumn("START_TIME", timestampType, tx -> tx.startTime)
                .<String>addColumn("TYPE", stringType, tx -> tx.type)
                .<String>addColumn("PRIORITY", stringType, tx -> tx.priority)
                .dataProvider(publisher)
                .build();
    }

    static class TxInfo {
        private final String id;
        private final @Nullable String state;
        private final Instant startTime;
        private final String type;
        private final String priority;

        static TxInfo forReadOnly(UUID id) {
            return new TxInfo(id, null, true);
        }

        static TxInfo forReadWrite(UUID id, TxState txState) {
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
