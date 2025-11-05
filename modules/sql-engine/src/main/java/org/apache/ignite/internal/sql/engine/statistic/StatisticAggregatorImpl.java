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

package org.apache.ignite.internal.sql.engine.statistic;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.isCompletedSuccessfully;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.replicator.PartitionModificationInfo;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.GetEstimatedSizeWithLastModifiedTsRequest;
import org.apache.ignite.internal.replicator.message.GetEstimatedSizeWithLastModifiedTsResponse;
import org.apache.ignite.internal.replicator.message.PartitionModificationInfoMessage;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.table.InternalTable;
import org.jetbrains.annotations.Nullable;

/** Statistic aggregator. */
public class StatisticAggregatorImpl implements
        StatisticAggregator<Collection<InternalTable>, CompletableFuture<Int2ObjectMap<PartitionModificationInfo>>> {
    private static final IgniteLogger LOG = Loggers.forClass(StatisticAggregatorImpl.class);
    private final Supplier<Set<LogicalNode>> clusterNodes;
    private final MessagingService messagingService;
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();
    private static final long REQUEST_ESTIMATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);
    private final AtomicReference<@Nullable Map<TablePartitionId, CompletableFuture<Object>>> requestsCompletion = new AtomicReference<>();

    /** Constructor. */
    public StatisticAggregatorImpl(
            Supplier<Set<LogicalNode>> clusterNodes,
            MessagingService messagingService
    ) {
        this.clusterNodes = clusterNodes;
        this.messagingService = messagingService;

        messagingService.addMessageHandler(ReplicaMessageGroup.class, this::handleMessage);
    }

    private void handleMessage(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
        Map<TablePartitionId, CompletableFuture<Object>> completedRequests = requestsCompletion.get();

        if (message instanceof GetEstimatedSizeWithLastModifiedTsResponse && completedRequests != null) {
            GetEstimatedSizeWithLastModifiedTsResponse response = (GetEstimatedSizeWithLastModifiedTsResponse) message;
            for (PartitionModificationInfoMessage ent : response.modifications()) {
                TablePartitionId id = new TablePartitionId(ent.tableId(), ent.partId());
                long estSize = ent.estimatedSize();
                long modificationCounter = ent.lastModificationCounter();

                CompletableFuture<Object> responseFut = completedRequests.get(id);

                // stale response
                if (responseFut == null) {
                    continue;
                }

                synchronized (this) {
                    if (isCompletedSuccessfully(responseFut)) {
                        PartitionModificationInfo res = (PartitionModificationInfo) responseFut.join();
                        if (modificationCounter > res.lastModificationCounter()) {
                            responseFut.complete(completedFuture(new PartitionModificationInfo(estSize, modificationCounter)));
                        }
                    } else {
                        responseFut.complete(new PartitionModificationInfo(estSize, modificationCounter));
                    }
                }
            }
        }
    }

    /**
     * Returns future with map<<em>last modification timestamp</em>, <em>estimated size</em>> for input tables.
     */
    @Override
    public CompletableFuture<Int2ObjectMap<PartitionModificationInfo>> estimatedSizeWithLastUpdate(Collection<InternalTable> tables) {
        // some requests are in progress
        if (requestsCompletion.get() != null) {
            return completedFuture(Int2ObjectMaps.emptyMap());
        }

        Collection<Integer> tablesId = tables.stream().map(InternalTable::tableId).collect(Collectors.toList());

        GetEstimatedSizeWithLastModifiedTsRequest request =
                REPLICA_MESSAGES_FACTORY.getEstimatedSizeWithLastModifiedTsRequest().tables(tablesId).build();

        HashMap<TablePartitionId, CompletableFuture<Object>> partIdRequests = new HashMap<>();
        requestsCompletion.set(partIdRequests);

        for (InternalTable t : tables) {
            for (int p = 0; p < t.partitions(); ++p) {
                partIdRequests.put(new TablePartitionId(t.tableId(), p),
                        new CompletableFuture<>().orTimeout(REQUEST_ESTIMATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
            }
        }

        List<CompletableFuture<Void>> reqFutures = new ArrayList<>();

        for (LogicalNode node : clusterNodes.get()) {
            CompletableFuture<Void> reqFut = messagingService.send(node, request);

            reqFutures.add(reqFut.orTimeout(REQUEST_ESTIMATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        }

        CompletableFuture<CompletableFuture<Void>>[] requests = reqFutures.toArray(CompletableFuture[]::new);

        CompletableFuture<Void> allRequests = allOf(requests);

        Int2ObjectMap<PartitionModificationInfo> summary = new Int2ObjectOpenHashMap<>();

        for (InternalTable t : tables) {
            Map<TablePartitionId, CompletableFuture<Object>> tableResponses = new HashMap<>();
            for (Map.Entry<TablePartitionId, CompletableFuture<Object>> ent : partIdRequests.entrySet()) {
                if (ent.getKey().tableId() == t.tableId()) {
                    tableResponses.put(ent.getKey(), ent.getValue());
                }
            }

            allRequests = allRequests
                    .thenCompose(r -> allOf(tableResponses.values().toArray(CompletableFuture[]::new)))
                    .handle((ret, ex) -> {
                        if (ex != null) {
                            LOG.debug("Can`t update statistics for table [id={}].", ex, t.tableId());
                        }

                        CompletableFuture<Void> allResponses = allOf(tableResponses.values().toArray(CompletableFuture[]::new));

                        if (!isCompletedSuccessfully(allResponses)) {
                            if (LOG.isDebugEnabled()) {
                                for (Map.Entry<TablePartitionId, CompletableFuture<Object>> ent : tableResponses.entrySet()) {
                                    if (isCompletedSuccessfully(ent.getValue())) {
                                        LOG.debug("Can`t update statistics for table partition [id={}].", ent.getKey());
                                    }
                                }
                            }
                            return null;
                        }

                        for (Map.Entry<TablePartitionId, CompletableFuture<Object>> ent : tableResponses.entrySet()) {
                            PartitionModificationInfo info = (PartitionModificationInfo) ent.getValue().join();
                            long estSize = info.getEstimatedSize();
                            long modificationCounter = info.lastModificationCounter();

                            summary.compute(ent.getKey().tableId(), (k, v) -> v == null
                                    ? new PartitionModificationInfo(estSize, modificationCounter)
                                    : new PartitionModificationInfo(v.getEstimatedSize() + estSize,
                                            Math.max(v.lastModificationCounter(), modificationCounter)));
                        }

                        return null;
                    });
        }

        return allRequests.handle((ret, ex) -> {
            if (ex != null) {
                LOG.debug("Exception during tables size estimation.", ex);
                return Int2ObjectMaps.emptyMap();
            }

            requestsCompletion.set(null);

            return summary;
        });
    }
}
