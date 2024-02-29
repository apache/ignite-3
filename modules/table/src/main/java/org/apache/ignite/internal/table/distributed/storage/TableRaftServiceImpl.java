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

package org.apache.ignite.internal.table.distributed.storage;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.table.TableRaftService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeResolver;
import org.jetbrains.annotations.TestOnly;

/**
 * Table's raft client storage.
 */
public class TableRaftServiceImpl implements TableRaftService {

    /** Mutex for the partition maps update. */
    private final Object updatePartitionMapsMux = new Object();

    /** Map update guarded by {@link #updatePartitionMapsMux}. */
    private volatile Int2ObjectMap<RaftGroupService> raftGroupServiceByPartitionId;

    /** Resolver that resolves a node consistent ID to cluster node. */
    private final ClusterNodeResolver clusterNodeResolver;

    /** Partitions. */
    private final int partitions;

    /** Table name. */
    private volatile String tableName;

    /**
     * Constructor.
     *
     * @param tableName Table name.
     * @param partitions Partitions.
     * @param partMap Map partition id to raft group.
     * @param clusterNodeResolver Cluster node resolver.
     */
    public TableRaftServiceImpl(
            String tableName,
            int partitions,
            Int2ObjectMap<RaftGroupService> partMap,
            ClusterNodeResolver clusterNodeResolver
    ) {
        this.tableName = tableName;
        this.partitions = partitions;
        this.raftGroupServiceByPartitionId = partMap;
        this.clusterNodeResolver = clusterNodeResolver;
    }

    @Override
    public ClusterNode leaderAssignment(int partition) {
        awaitLeaderInitialization();

        RaftGroupService raftGroupService = raftGroupServiceByPartitionId.get(partition);
        if (raftGroupService == null) {
            throw new IgniteInternalException("No such partition " + partition + " in table " + tableName);
        }

        return clusterNodeResolver.getByConsistentId(raftGroupService.leader().consistentId());
    }

    /** {@inheritDoc} */
    @Override
    public RaftGroupService partitionRaftGroupService(int partition) {
        RaftGroupService raftGroupService = raftGroupServiceByPartitionId.get(partition);
        if (raftGroupService == null) {
            throw new IgniteInternalException("No such partition " + partition + " in table " + tableName);
        }

        return raftGroupService;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        for (RaftGroupService srv : raftGroupServiceByPartitionId.values()) {
            srv.shutdown();
        }
    }

    public void name(String newName) {
        this.tableName = newName;
    }

    /**
     * Updates internal table raft group service for given partition.
     *
     * @param p Partition.
     * @param raftGrpSvc Raft group service.
     */
    public void updateInternalTableRaftGroupService(int p, RaftGroupService raftGrpSvc) {
        RaftGroupService oldSrvc;

        synchronized (updatePartitionMapsMux) {
            Int2ObjectMap<RaftGroupService> newPartitionMap = new Int2ObjectOpenHashMap<>(partitions);

            newPartitionMap.putAll(raftGroupServiceByPartitionId);

            oldSrvc = newPartitionMap.put(p, raftGrpSvc);

            raftGroupServiceByPartitionId = newPartitionMap;
        }

        if (oldSrvc != null && oldSrvc != raftGrpSvc) {
            oldSrvc.shutdown();
        }
    }

    /**
     * Returns map of partition -> list of peers and learners of that partition.
     */
    @TestOnly
    public Map<Integer, List<String>> peersAndLearners() {
        awaitLeaderInitialization();

        return raftGroupServiceByPartitionId.int2ObjectEntrySet().stream()
                .collect(Collectors.toMap(Entry::getIntKey, e -> {
                    RaftGroupService service = e.getValue();
                    return Stream.of(service.peers(), service.learners())
                            .filter(Objects::nonNull)
                            .flatMap(Collection::stream)
                            .map(Peer::consistentId)
                            .collect(Collectors.toList());
                }));
    }

    private void awaitLeaderInitialization() {
        List<CompletableFuture<Void>> futs = new ArrayList<>();

        for (RaftGroupService raftSvc : raftGroupServiceByPartitionId.values()) {
            if (raftSvc.leader() == null) {
                futs.add(raftSvc.refreshLeader());
            }
        }

        CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new)).join();
    }
}
