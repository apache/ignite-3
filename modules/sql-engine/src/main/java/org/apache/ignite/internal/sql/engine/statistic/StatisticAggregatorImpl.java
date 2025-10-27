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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.hlc.HybridTimestamp;
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
        StatisticAggregator<Collection<InternalTable>, CompletableFuture<Map<Integer, PartitionModificationInfo>>> {
    private static final IgniteLogger LOG = Loggers.forClass(StatisticAggregatorImpl.class);
    private final Supplier<Set<LogicalNode>> clusterNodes;
    private final Supplier<HybridTimestamp> currentClock;
    private final MessagingService messagingService;
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();
    private static final long REQUEST_ESTIMATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(3);
    private final ConcurrentMap<TablePartitionId, CompletableFuture<Object>> requestsCompletion = new ConcurrentHashMap<>();

    /** Constructor. */
    public StatisticAggregatorImpl(
            Supplier<Set<LogicalNode>> clusterNodes,
            Supplier<HybridTimestamp> currentClock,
            MessagingService messagingService
    ) {
        this.clusterNodes = clusterNodes;
        this.currentClock = currentClock;
        this.messagingService = messagingService;

        messagingService.addMessageHandler(ReplicaMessageGroup.class, this::handleMessage);
    }

    private void handleMessage(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
        if (message instanceof GetEstimatedSizeWithLastModifiedTsResponse && !requestsCompletion.isEmpty()) {
            GetEstimatedSizeWithLastModifiedTsResponse response = (GetEstimatedSizeWithLastModifiedTsResponse) message;
            for (PartitionModificationInfoMessage ent : response.modifications()) {
                TablePartitionId id = new TablePartitionId(ent.tableId(), ent.partId());
                long estSize = ent.estimatedSize();
                long modificationCounter = ent.lastModificationCounter();

                CompletableFuture<Object> responseFut = requestsCompletion.get(id);

                // stale response
                if (responseFut == null) {
                    continue;
                }

                if (isCompletedSuccessfully(responseFut)) {
                    PartitionModificationInfo res = (PartitionModificationInfo) responseFut.join();
                    if (modificationCounter > res.lastModificationCounter()) {
                        requestsCompletion.replace(id, responseFut,
                                completedFuture(new PartitionModificationInfo(estSize, modificationCounter)));
                    }
                } else {
                    responseFut.complete(new PartitionModificationInfo(estSize, modificationCounter));
                }
            }
        }
    }

    /**
     * Returns the map<<em>last modification timestamp</em>, <em>estimated size</em>> for input tables.
     */
    @Override
    public CompletableFuture<Map<Integer, PartitionModificationInfo>> estimatedSizeWithLastUpdate(Collection<InternalTable> tables) {
        Collection<Integer> tablesId = tables.stream().map(InternalTable::tableId).collect(Collectors.toList());

        GetEstimatedSizeWithLastModifiedTsRequest request =
                REPLICA_MESSAGES_FACTORY.getEstimatedSizeWithLastModifiedTsRequest().tables(tablesId).build();

        List<CompletableFuture<Void>> reqFutures = new ArrayList<>();

        for (InternalTable t : tables) {
            for (int p = 0; p < t.partitions(); ++p) {
                requestsCompletion.put(new TablePartitionId(t.tableId(), p),
                        new CompletableFuture<>().orTimeout(REQUEST_ESTIMATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
            }
        }

        for (LogicalNode node : clusterNodes.get()) {
            CompletableFuture<Void> reqFut = messagingService.send(node, request);

            reqFutures.add(reqFut.orTimeout(REQUEST_ESTIMATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        }

        CompletableFuture<CompletableFuture<Void>>[] requests = reqFutures.toArray(CompletableFuture[]::new);

        CompletableFuture<CompletableFuture<Object>>[] responses = requestsCompletion.values().toArray(CompletableFuture[]::new);

        return allOf(requests)
                .thenCompose(r -> allOf(responses))
                .thenApply(unused -> {
                    Map<Integer, PartitionModificationInfo> summary = new Int2ObjectOpenHashMap<>();

                    for (Map.Entry<TablePartitionId, CompletableFuture<Object>> ent : requestsCompletion.entrySet()) {
                        TablePartitionId partitionPerTableId = ent.getKey();
                        CompletableFuture<Object> val = ent.getValue();

                        if (isCompletedSuccessfully(val)) {
                            PartitionModificationInfo info = (PartitionModificationInfo) val.join();
                            long estSize = info.getEstimatedSize();
                            long modificationCounter = info.lastModificationCounter();

                            summary.compute(partitionPerTableId.tableId(), (k, v) -> v == null
                                    ? new PartitionModificationInfo(estSize, modificationCounter)
                                    : new PartitionModificationInfo(v.getEstimatedSize() + estSize,
                                            Math.max(v.lastModificationCounter(), modificationCounter)));
                        } else {
                            LOG.debug("Can`t update statistics for table partition [id={}].", partitionPerTableId);
                        }
                    }

                    return summary;
                })
                .whenComplete((res, ex) -> {
                    requestsCompletion.clear();
                });
    }
}
