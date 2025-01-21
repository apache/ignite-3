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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.TransformingIterator;

/**
 * {@code TRANSACTIONS} data view provider.
 */
public class TransactionViewDataProvider {
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

    public Iterable<TxInfo> dataSource() {
        return dataSource;
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
                                    && !isFinalState(e.getValue().txState()))
                            .map(e -> TxInfo.readWrite(e.getKey(), e.getValue().txState()))
                            .iterator()
            );
        }
    }

}
