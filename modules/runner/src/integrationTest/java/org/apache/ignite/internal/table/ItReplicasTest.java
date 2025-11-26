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

package org.apache.ignite.internal.table;

import static com.google.common.base.Predicates.notNull;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET_ALL;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyMultiRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlySingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicaTestUtils;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.thread.PublicApiThreading.ApiEntryRole;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.table.QualifiedName;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

class ItReplicasTest extends ClusterPerTestIntegrationTest {

    @Test
    void testLearnerReplicaCreatedAfterStartingNewNode() {
        executeSql("CREATE ZONE TEST_ZONE (PARTITIONS 1, REPLICAS ALL, QUORUM SIZE 2) STORAGE PROFILES ['default']");
        executeSql("CREATE TABLE TEST (id INT PRIMARY KEY, name INT) ZONE TEST_ZONE");
        executeSql("INSERT INTO TEST VALUES (0, 0)");

        await().untilAsserted(() -> {
            assertTrue(cluster.runningNodes().map(resolvePartition("TEST")).allMatch(notNull()),
                    "all nodes should contain table partition replica");
            Set<Assignment> stableAssignments = stablePartitionAssignments(cluster.node(0), "TEST");
            assertFalse(stableAssignments.stream().anyMatch(a -> !a.isPeer()), "no learners before starting new node");
        });

        Ignite newNode = cluster.startNode((int) cluster.runningNodes().count()); // starting a new node adds a learner

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Set<Assignment> stableAssignments = stablePartitionAssignments(cluster.node(0), "TEST");
            assertTrue(stableAssignments.stream().anyMatch(a -> !a.isPeer()), "learners found after staring new node");

            Set<String> stableConsistentIds = stableAssignments.stream().map(Assignment::consistentId).collect(Collectors.toSet());
            assertThat("new node in stable", stableConsistentIds, hasItem(newNode.name()));
            assertThat(stableConsistentIds, containsInAnyOrder(cluster.runningNodes().map(Ignite::name).toArray(String[]::new)));

            assertTrue(cluster.runningNodes()
                    .filter(in(stableAssignments))
                    .map(toRaftClient("TEST"))
                    .allMatch(equalsPeersAndLearners(stableAssignments)),
                    "peers and learners on every assigned nodes should be equal"
            );

            assertTrue(cluster.runningNodes()
                    .filter(in(stableAssignments))
                    .map(resolvePartition("TEST"))
                    .allMatch(notNull()),
                    "table partition replica should exist on every assigned nodes"
            );

            assertTrue(cluster.runningNodes()
                    .filter(notIn(stableAssignments))
                    .noneMatch(isReplicationGroupStarted("TEST")),
                    "not assigned nodes should not be in table replication group"
            );
        });
    }

    @Test
    void testLearnerReplicaReadOnlyRequestCanReadData() {
        PublicApiThreading.setThreadRole(ApiEntryRole.SYNC_PUBLIC_API);
        executeSql("CREATE ZONE TEST_ZONE (PARTITIONS 1, REPLICAS ALL, QUORUM SIZE 2) STORAGE PROFILES ['default']");
        executeSql("CREATE TABLE TEST (id INT PRIMARY KEY, name INT) ZONE TEST_ZONE");
        executeSql("INSERT INTO TEST VALUES (1, 2)");

        cluster.startNode((int) cluster.runningNodes().count()); // starting a new node adds a learner
        AtomicReference<IgniteImpl> learnerRef = new AtomicReference<>();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Set<Assignment> stableAssignments = stablePartitionAssignments(cluster.node(0), "TEST");
            String learner = stableAssignments.stream().filter(a -> !a.isPeer()).map(Assignment::consistentId).findFirst().orElse(null);
            assertTrue(learner != null && !learner.isBlank(), "learners found after staring new node");
            IgniteImpl learnerNode = findNode(node -> node.name().equals(learner));
            assertTrue(equalsPeersAndLearners(stableAssignments).test(toRaftClient("TEST").apply(learnerNode)),
                    "learner raft client peers and learners equal to stable assignments");
            learnerRef.set(learnerNode);
        });

        var r = new ReplicaRequests<>(learnerRef.get(), "TEST_ZONE", "TEST", 0, Integer.class, Integer.class);

        var expected = new IgniteBiTuple<>(1, 2);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertEquals(expected, r.readOnlySingleRowPkReplicaRequest(1).invokeAndGetFirstRow());
        });
        assertEquals(expected, r.readOnlyMultiRowPkReplicaRequest(1).invokeAndGetFirstRow());
        assertEquals(expected, r.readOnlyScanRetrieveBatchReplicaRequest().invokeAndGetFirstRow());

        executeSql("INSERT INTO TEST VALUES (2, 3)");
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertEquals(new IgniteBiTuple<>(2, 3), r.readOnlySingleRowPkReplicaRequest(2).invokeAndGetFirstRow(),
                    "new inserted value replicated eventually");
        });
    }

    private static Set<Assignment> stablePartitionAssignments(Ignite node, String tableName) {
        IgniteImpl ignite = unwrapIgniteImpl(node);
        int zoneId = getZoneId(ignite, tableName);

        return ZoneRebalanceUtil.zoneStableAssignments(ignite.metaStorageManager(), zoneId, new int[]{0}).join().get(0).nodes();
    }

    private static ZonePartitionId partitionGroupId(Ignite node, String zoneName, String tableName, int partId) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node);
        CatalogManager catalogManager = igniteImpl.catalogManager();
        Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

        return new ZonePartitionId(catalog.zone(zoneName).id(), partId);
    }

    private static int getZoneId(Ignite node, String tableName) {
        IgniteImpl ignite = unwrapIgniteImpl(node);
        return TableTestUtils.getZoneIdByTableNameStrict(ignite.catalogManager(), tableName, ignite.clock().nowLong());
    }

    private static Function<Ignite, @Nullable RowId> resolvePartition(String tableName) {
        return node -> {
            int partitionId = 0;
            QualifiedName qualifiedName = QualifiedName.fromSimple(tableName);
            TableManager tableManager = unwrapTableManager(node.tables());

            MvPartitionStorage storage = tableManager.tableView(qualifiedName)
                    .internalTable()
                    .storage()
                    .getMvPartition(partitionId);

            if (storage == null) {
                return null;
            }

            return bypassingThreadAssertions(() -> storage.closestRowId(RowId.lowestRowId(partitionId)));
        };
    }

    private static Function<Ignite, RaftGroupService> toRaftClient(String tableName) {
        return node -> {
            ReplicaManager replicaManager = unwrapIgniteImpl(node).replicaManager();
            int zoneId = getZoneId(node, tableName);
            return ReplicaTestUtils.getRaftClient(replicaManager, zoneId, 0)
                    .orElseThrow(() -> new AssertionError(
                            format("No raft client found for table {} in node {}", tableName, node.name())
                    ));
        };
    }

    private static Function<Ignite, ReplicaListener> toReplicaListener(String zoneName, String tableName, int partId) {
        return node -> {
            try {
                CompletableFuture<Replica> replicaFut = unwrapIgniteImpl(node).replicaManager()
                        .replica(partitionGroupId(node, zoneName, tableName, partId));

                if (replicaFut == null) {
                    throw new AssertionError(format("Replica not found for table {} in node {}", tableName, node.name()));
                }

                Replica replica = replicaFut.get(30, TimeUnit.SECONDS);
                return replica.listener();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };
    }

    private static Predicate<RaftGroupService> equalsPeersAndLearners(Set<Assignment> assignments) {
        return client -> {
            PeersAndLearners expected = PeersAndLearners.fromAssignments(assignments);
            PeersAndLearners actual = PeersAndLearners.fromPeers(requireNonNull(client.peers()), requireNonNull(client.learners()));
            return expected.equals(actual);
        };
    }

    private static Predicate<Ignite> isReplicationGroupStarted(String tableName) {
        return node -> {
            ZonePartitionId partId = new ZonePartitionId(getZoneId(node, tableName), 0);
            return unwrapIgniteImpl(node).replicaManager().isReplicaStarted(partId);
        };
    }

    private static Predicate<Ignite> notIn(Set<Assignment> assignments) {
        return in(assignments).negate();
    }

    private static Predicate<Ignite> in(Set<Assignment> assignments) {
        return node -> {
            String nodeName = node.name();
            return assignments.stream().anyMatch(a -> a.consistentId().equals(nodeName));
        };
    }

    static class ReplicaRequests<K, V> {
        private final IgniteImpl node;
        private final SchemaDescriptor schema;
        private final String zoneName;
        private final String tableName;
        private final int partId;
        private final KvMarshaller<K, V> marshaller;
        private final ReplicaListener replicaListener;

        ReplicaRequests(IgniteImpl node, String zoneName, String tableName, int partId, Class<K> keyClass, Class<V> valClass) {
            this.node = node;
            this.zoneName = zoneName;
            this.tableName = tableName;
            this.partId = partId;
            this.schema = unwrapTableViewInternal(node.tables().table("TEST")).schemaView().lastKnownSchema();
            this.marshaller = new ReflectionMarshallerFactory().create(schema, keyClass, valClass);
            this.replicaListener = of(node).map(toReplicaListener(zoneName, tableName, partId)).orElseThrow();
        }

        Request readOnlySingleRowPkReplicaRequest(K pk) {
            InternalClusterNode clusterNode = node.clusterService().topologyService().localMember();
            TableViewInternal table = unwrapTableViewInternal(node.tables().table(tableName));
            InternalTransaction tx = node.txManager().beginImplicitRo(node.observableTimeTracker());
            Row pkRow = marshaller.marshal(pk);

            ReadOnlySingleRowPkReplicaRequest request = new PartitionReplicationMessagesFactory()
                    .readOnlySingleRowPkReplicaRequest()
                    .groupId(groupIdMessage(node, zoneName, tableName, partId))
                    .tableId(table.tableId())
                    .readTimestamp(node.clock().now())
                    .schemaVersion(pkRow.schemaVersion())
                    .primaryKey(pkRow.tupleSlice())
                    .transactionId(tx.id())
                    .coordinatorId(clusterNode.id())
                    .requestType(RequestType.RO_GET)
                    .build();
            return new Request(request);
        }

        Request readOnlyMultiRowPkReplicaRequest(K... pk) {
            InternalClusterNode clusterNode = node.clusterService().topologyService().localMember();
            TableViewInternal table = unwrapTableViewInternal(node.tables().table(tableName));
            InternalTransaction tx = node.txManager().beginImplicitRo(node.observableTimeTracker());
            List<ByteBuffer> buffers = Arrays.stream(pk).map(marshaller::marshal).map(BinaryRow::tupleSlice).collect(Collectors.toList());
            Row pkRow = marshaller.marshal(pk[0]);

            ReadOnlyMultiRowPkReplicaRequest request = new PartitionReplicationMessagesFactory()
                    .readOnlyMultiRowPkReplicaRequest()
                    .groupId(groupIdMessage(node, zoneName, tableName, partId))
                    .tableId(table.tableId())
                    .readTimestamp(node.clock().now())
                    .schemaVersion(pkRow.schemaVersion())
                    .primaryKeys(buffers)
                    .transactionId(tx.id())
                    .coordinatorId(clusterNode.id())
                    .requestType(RO_GET_ALL)
                    .build();
            return new Request(request);
        }

        Request readOnlyScanRetrieveBatchReplicaRequest() {
            InternalClusterNode clusterNode = node.clusterService().topologyService().localMember();
            TableViewInternal table = unwrapTableViewInternal(node.tables().table(tableName));
            InternalTransaction tx = node.txManager().beginImplicitRo(node.observableTimeTracker());

            ReadOnlyScanRetrieveBatchReplicaRequest request = new PartitionReplicationMessagesFactory()
                    .readOnlyScanRetrieveBatchReplicaRequest()
                    .groupId(groupIdMessage(node, zoneName, tableName, partId))
                    .tableId(table.tableId())
                    .readTimestamp(node.clock().now())
                    .scanId(1)
                    .batchSize(100)
                    .transactionId(tx.id())
                    .coordinatorId(clusterNode.id())
                    .build();
            return new Request(request);
        }

        private static ReplicationGroupIdMessage groupIdMessage(IgniteImpl node, String zoneName, String tableName, int partId) {
            PartitionGroupId partitionGroupId = partitionGroupId(node, zoneName, tableName, partId);
            var replicaMessagesFactory = new ReplicaMessagesFactory();
            return toZonePartitionIdMessage(replicaMessagesFactory, ((ZonePartitionId) partitionGroupId));
        }

        class Request {
            private final ReadOnlyReplicaRequest request;

            Request(ReadOnlyReplicaRequest request) {
                this.request = request;
            }

            IgniteBiTuple<K, V> invokeAndGetFirstRow() {
                PublicApiThreading.setThreadRole(ApiEntryRole.SYNC_PUBLIC_API);

                InternalClusterNode sender = node.clusterService().topologyService().localMember();
                CompletableFuture<ReplicaResult> fut = replicaListener.invoke(request, sender.id());

                assertThat(fut, willCompleteSuccessfully());

                Row row;
                if (request instanceof ReadOnlySingleRowPkReplicaRequest) {
                    row = Row.wrapBinaryRow(schema, (BinaryRow) fut.join().result());
                } else if (request instanceof ReadOnlyMultiRowPkReplicaRequest) {
                    row = Row.wrapBinaryRow(schema, ((List<BinaryRow>) fut.join().result()).get(0));
                } else if (request instanceof ReadOnlyScanRetrieveBatchReplicaRequest) {
                    row = Row.wrapBinaryRow(schema, ((List<BinaryRow>) fut.join().result()).get(0));
                } else {
                    throw new AssertionError("Unknown request type: " + request.getClass());
                }

                K k = marshaller.unmarshalKey(row);
                V v = marshaller.unmarshalValue(row);
                return new IgniteBiTuple<>(k, v);
            }
        }

    }

}
