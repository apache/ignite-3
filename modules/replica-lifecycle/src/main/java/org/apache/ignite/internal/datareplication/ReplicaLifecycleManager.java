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

package org.apache.ignite.internal.datareplication;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_CREATE;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.datareplication.snapshot.FailFastSnapshotStorageFactory;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.TestOnly;

/**
 * The main responsibilities of this class:
 * - Start the appropriate replication nodes on the zone creation.
 * - Stop the same entities on the zone removing.
 * - Support the rebalance mechanism and start the new replication entities when the rebalance triggers occurred.
 */
public class ReplicaLifecycleManager implements IgniteComponent {

    /* Feature flag for zone based collocation track */
    // TODO IGNITE-22115 remove it
    public static final boolean ENABLED = getBoolean("IGNITE_ZONE_BASED_REPLICATION", true);

    /** Catalog manager */
    private final CatalogManager catalogMgr;

    private final RaftManager raftMgr;

    private final ReplicaManager replicaMgr;

    private final TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory;

    private final DistributionZoneManager distributionZoneMgr;

    private final MetaStorageManager metaStorageMgr;

    private final TopologyService topologyService;

    private final Marshaller raftCommandsMarshaller;

    private final ClockService clockService;

    private final PlacementDriver placementDriver;

    public static final String ZONE_STABLE_ASSIGNMENTS_PREFIX = "zone.assignments.stable.";

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ReplicaLifecycleManager.class);

    private final Set<ReplicationGroupId> replicationGroupIds = ConcurrentHashMap.newKeySet();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    @TestOnly
    public ReplicaLifecycleManager(CatalogManager catalogMgr, RaftManager raftMgr, ReplicaManager replicaMgr,
            TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory, DistributionZoneManager distributionZoneMgr,
            MetaStorageManager metaStorageMgr, TopologyService topologyService,
            MessageSerializationRegistry messageSerializationRegistry, ClockService clockService, PlacementDriver placementDriver) {
        this.catalogMgr = catalogMgr;
        this.raftMgr = raftMgr;
        this.replicaMgr = replicaMgr;
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.distributionZoneMgr = distributionZoneMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.topologyService = topologyService;
        this.raftCommandsMarshaller = new ThreadLocalPartitionCommandsMarshaller(messageSerializationRegistry);
        this.clockService = clockService;
        this.placementDriver = placementDriver;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        if (!ENABLED) {
            return nullCompletedFuture();
        }

        catalogMgr.listen(ZONE_CREATE,
                (CreateZoneEventParameters parameters) ->
                        inBusyLock(busyLock, () -> onCreateZone(parameters).thenApply((ignored) -> false))
        );

        // TODO: IGNITE-22231 Will be replaced by the real code.
        return nullCompletedFuture();
    }

    private CompletableFuture<Void> onCreateZone(CreateZoneEventParameters createZoneEventParameters) {

        return inBusyLockAsync(busyLock, () -> {
            CompletableFuture<List<Assignments>> assignmentsFuture = getOrCreateAssignments(
                    createZoneEventParameters.zoneDescriptor(),
                    createZoneEventParameters.causalityToken(),
                    createZoneEventParameters.catalogVersion()
            );

            CompletableFuture<List<Assignments>> assignmentsFutureAfterInvoke =
                    writeTableAssignmentsToMetastore(createZoneEventParameters.zoneDescriptor().id(), assignmentsFuture);

            return createZoneReplicationEntities(assignmentsFutureAfterInvoke, createZoneEventParameters.zoneDescriptor().id());
        });
    }

    public CompletableFuture<Void> createZoneReplicationEntities(CompletableFuture<List<Assignments>> assignmentsFuture, int zoneId) {
        return assignmentsFuture.thenCompose(assignments -> {
            assert assignments != null : IgniteStringFormatter.format("Zone [id={}] has empty assignments.", zoneId);

            List<CompletableFuture<?>> partitionsStartFutures = new ArrayList<>();

            for (int i = 0; i < assignments.size(); i++) {
                int partId = i;

                partitionsStartFutures.add(createZonePartitionReplicationNodes(zoneId, partId, assignments.get(i)));
            }

            return allOf(partitionsStartFutures.toArray(new CompletableFuture<?>[0]));
        });
    }

    public CompletableFuture<Void> createZonePartitionReplicationNodes(int zoneId, int partId, Assignments assignments) {

        Assignment localMemberAssignment = assignments.nodes().stream()
                .filter(a -> a.consistentId().equals(localNode().name()))
                .findAny()
                .orElse(null);

        if (localMemberAssignment == null) {
            return nullCompletedFuture();
        }

        PeersAndLearners realConfiguration = configurationFromAssignments(assignments.nodes());

        Peer serverPeer = realConfiguration.peer(localNode().name());

        ZonePartitionId replicaGrpId = new ZonePartitionId(zoneId, partId);

        var raftNodeId = new RaftNodeId(replicaGrpId, serverPeer);

        // TODO https://issues.apache.org/jira/browse/IGNITE-22391 support for volatile stores
        RaftGroupOptions raftGroupOptions = RaftGroupOptions.forPersistentStores();

        raftGroupOptions.snapshotStorageFactory(new FailFastSnapshotStorageFactory());

        raftGroupOptions.commandsMarshaller(raftCommandsMarshaller);

        RaftGroupListener raftGroupListener = new PartitionGroupListener();

        try {
            CompletableFuture<TopologyAwareRaftGroupService> raftClient =  ((Loza) raftMgr).startRaftGroupNode(
                    raftNodeId,
                    realConfiguration,
                    raftGroupListener,
                    RaftGroupEventsListener.noopLsnr,
                    raftGroupOptions,
                    raftGroupServiceFactory
            );

            return raftClient.thenCompose(client ->
                replicaMgr.startReplica(
                        replicaGrpId,
                        new ZonePartitionReplicaListener(),
                        client
                )).thenRun(() -> replicationGroupIds.add(replicaGrpId));
        } catch (NodeStoppingException e) {
            throw new RuntimeException(e);
        }
    }

    private static PeersAndLearners configurationFromAssignments(Collection<Assignment> assignments) {
        var peers = new HashSet<String>();
        var learners = new HashSet<String>();

        for (Assignment assignment : assignments) {
            if (assignment.isPeer()) {
                peers.add(assignment.consistentId());
            } else {
                learners.add(assignment.consistentId());
            }
        }

        return PeersAndLearners.fromConsistentIds(peers, learners);
    }

    private ClusterNode localNode() {
        return topologyService.localMember();
    }

    @Override
    public void beforeNodeStop() {
        for (ReplicationGroupId replicationGroupId : replicationGroupIds) {
            try {
                raftMgr.stopRaftNodes(replicationGroupId);
                replicaMgr.stopReplica(replicationGroupId);
            } catch (NodeStoppingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Writes the set of assignments to meta storage. If there are some assignments already, gets them from meta storage. Returns
     * the list of assignments that really are in meta storage.
     *
     * @param zoneId  Zone id.
     * @param assignmentsFuture Assignments future, to get the assignments that should be written.
     * @return Real list of assignments.
     */
    public CompletableFuture<List<Assignments>> writeTableAssignmentsToMetastore(
            int zoneId,
            CompletableFuture<List<Assignments>> assignmentsFuture
    ) {
        return assignmentsFuture.thenCompose(newAssignments -> {
            assert !newAssignments.isEmpty();

            List<Operation> partitionAssignments = new ArrayList<>(newAssignments.size());

            for (int i = 0; i < newAssignments.size(); i++) {
                ByteArray stableAssignmentsKey = zoneStablePartAssignmentsKey(new ZonePartitionId(zoneId, i));
                byte[] anAssignment = newAssignments.get(i).toBytes();
                Operation op = put(stableAssignmentsKey, anAssignment);
                partitionAssignments.add(op);
            }

            Condition condition = notExists(new ByteArray(partitionAssignments.get(0).key()));

            return metaStorageMgr
                    .invoke(condition, partitionAssignments, Collections.emptyList())
                    .handle((invokeResult, e) -> {
                        if (e != null) {
                            LOG.error(
                                    "Couldn't write assignments [assignmentsList={}] to metastore during invoke.",
                                    e,
                                    Assignments.assignmentListToString(newAssignments)
                            );

                            throw ExceptionUtils.sneakyThrow(e);
                        }

                        return invokeResult;
                    })
                    .thenCompose(invokeResult -> {
                        if (invokeResult) {
                            LOG.info(
                                    "Assignments calculated from data nodes are successfully written to meta storage"
                                            + " [zoneId={}, assignments={}].",
                                    zoneId,
                                    Assignments.assignmentListToString(newAssignments)
                            );

                            return completedFuture(newAssignments);
                        } else {
                            Set<ByteArray> partKeys = IntStream.range(0, newAssignments.size())
                                    .mapToObj(p -> zoneStablePartAssignmentsKey(new ZonePartitionId(zoneId, p)))
                                    .collect(toSet());

                            CompletableFuture<Map<ByteArray, Entry>> resFuture = metaStorageMgr.getAll(partKeys);

                            return resFuture.thenApply(metaStorageAssignments -> {
                                List<Assignments> realAssignments = new ArrayList<>();

                                for (int p = 0; p < newAssignments.size(); p++) {
                                    var partId = new ZonePartitionId(zoneId, p);
                                    Entry assignmentsEntry = metaStorageAssignments.get(zoneStablePartAssignmentsKey(partId));

                                    assert assignmentsEntry != null && !assignmentsEntry.empty() && !assignmentsEntry.tombstone()
                                            : "Unexpected assignments for partition [" + partId + ", entry=" + assignmentsEntry + "].";

                                    Assignments real = Assignments.fromBytes(assignmentsEntry.value());

                                    realAssignments.add(real);
                                }

                                LOG.info(
                                        "Assignments picked up from meta storage [zoneId={}, assignments={}].",
                                        zoneId,
                                        Assignments.assignmentListToString(realAssignments)
                                );

                                return realAssignments;
                            });
                        }
                    })
                    .handle((realAssignments, e) -> {
                        if (e != null) {
                            LOG.error("Couldn't get assignments from metastore for zone [zoneId={}].", e, zoneId);

                            throw ExceptionUtils.sneakyThrow(e);
                        }

                        return realAssignments;
                    });
        });
    }

    /**
     * Check if the table already has assignments in the meta storage locally.
     * So, it means, that it is a recovery process and we should use the meta storage local assignments instead of calculation
     * of the new ones.
     */
    private CompletableFuture<List<Assignments>> getOrCreateAssignments(
            CatalogZoneDescriptor zoneDescriptor,
            long causalityToken,
            int catalogVersion
    ) {
        CompletableFuture<List<Assignments>> assignmentsFuture;

        if (zonePartitionAssignmentsGetLocally(metaStorageMgr, zoneDescriptor.id(), 0, causalityToken) != null) {
            assignmentsFuture = completedFuture(
                    zoneAssignmentsGetLocally(metaStorageMgr, zoneDescriptor.id(), zoneDescriptor.partitions(), causalityToken));
        } else {
            assignmentsFuture = distributionZoneMgr.dataNodes(causalityToken, catalogVersion, zoneDescriptor.id())
                    .thenApply(dataNodes -> AffinityUtils.calculateAssignments(
                            dataNodes,
                            zoneDescriptor.partitions(),
                            zoneDescriptor.replicas()
                    ).stream().map(Assignments::of).collect(toList()));

            assignmentsFuture.thenAccept(assignmentsList -> LOG.info(
                    "Assignments calculated from data nodes [zone={}, zoneId={}, assignments={}, revision={}]",
                    zoneDescriptor.name(),
                    zoneDescriptor.id(),
                    Assignments.assignmentListToString(assignmentsList),
                    causalityToken
            ));
        }

        return assignmentsFuture;
    }

    public static Set<Assignment> zonePartitionAssignmentsGetLocally(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int partitionNumber,
            long revision
    ) {
        Entry entry = metaStorageManager.getLocally(zoneStablePartAssignmentsKey(new ZonePartitionId(zoneId, partitionNumber)), revision);

        return (entry == null || entry.empty() || entry.tombstone()) ? null : Assignments.fromBytes(entry.value()).nodes();
    }

    public static ByteArray zoneStablePartAssignmentsKey(ZonePartitionId partId) {
        return new ByteArray(ZONE_STABLE_ASSIGNMENTS_PREFIX + partId);
    }

    public static List<Assignments> zoneAssignmentsGetLocally(
            MetaStorageManager metaStorageManager,
            int zoneId,
            int numberOfPartitions,
            long revision
    ) {
        return IntStream.range(0, numberOfPartitions)
                .mapToObj(p -> {
                    Entry e = metaStorageManager.getLocally(zoneStablePartAssignmentsKey(new ZonePartitionId(zoneId, p)), revision);

                    assert e != null && !e.empty() && !e.tombstone() : e;

                    return Assignments.fromBytes(e.value());
                })
                .collect(toList());
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!ENABLED) {
            return nullCompletedFuture();
        }

        return nullCompletedFuture();
    }
}
