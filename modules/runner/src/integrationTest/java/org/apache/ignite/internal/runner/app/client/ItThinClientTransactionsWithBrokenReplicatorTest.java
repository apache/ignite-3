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

package org.apache.ignite.internal.runner.app.client;

import static org.apache.ignite.internal.ReplicationGroupsUtils.zonePartitionIds;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.runner.app.client.ItThinClientTransactionsTest.generateKeysForPartition;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.AppendEntriesRequestImpl;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestProcessor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test thin client transactions failure handling with broken replicator.
 */
public class ItThinClientTransactionsWithBrokenReplicatorTest extends ItAbstractThinClientTest {
    @Override
    protected long raftTimeoutMillis() {
        return TimeUnit.SECONDS.toMillis(2); // Set small retry timeout to reduce the test execution time.
    }

    @Test
    public void testErrorDuringDirectMappingSinglePartitionTransaction() {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = unwrapIgniteImpl(server(0));
        IgniteImpl server1 = unwrapIgniteImpl(server(1));

        List<ZonePartitionId> replicationGroupIds = getPartitions(server0);
        ZonePartitionId part0 = replicationGroupIds.get(0);

        List<Tuple> tuples0 = generateKeysForPartition(100, 1, map, part0.partitionId(), table);

        FaultyAppendEntriesRequestProcessor proc0 = installFaultyAppendEntriesProcessor(server0);
        proc0.setFaultyGroup(part0);

        FaultyAppendEntriesRequestProcessor proc1 = installFaultyAppendEntriesProcessor(server1);
        proc1.setFaultyGroup(part0);

        KeyValueView<Tuple, Tuple> tupleView = table.keyValueView();

        Transaction tx = client().transactions().begin();
        tupleView.put(tx, tuples0.get(0), val(tuples0.get(0).intValue(0) + ""));

        try {
            tx.commit();
            fail("Expecting commit failure");
        } catch (TransactionException exception) {
            assertEquals(Transactions.TX_DELAYED_ACK_ERR, exception.code());
        }

        proc0.setFaultyGroup(null);
        proc1.setFaultyGroup(null);

        assertNull(tupleView.get(null, tuples0.get(0)));
    }

    @Test
    public void testErrorDuringDirectMappingSinglePartitionTransactionBatch() {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = unwrapIgniteImpl(server(0));
        IgniteImpl server1 = unwrapIgniteImpl(server(1));

        List<ZonePartitionId> replicationGroupIds = getPartitions(server0);
        ZonePartitionId part0 = replicationGroupIds.get(0);

        List<Tuple> tuples0 = generateKeysForPartition(200, 2, map, part0.partitionId(), table);

        Map<Tuple, Tuple> batch = new HashMap<>();

        for (Tuple tup : tuples0) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        FaultyAppendEntriesRequestProcessor proc0 = installFaultyAppendEntriesProcessor(server0);
        proc0.setFaultyGroup(part0);

        FaultyAppendEntriesRequestProcessor proc1 = installFaultyAppendEntriesProcessor(server1);
        proc1.setFaultyGroup(part0);

        KeyValueView<Tuple, Tuple> tupleView = table.keyValueView();

        Transaction tx = client().transactions().begin();
        tupleView.putAll(tx, batch);

        try {
            tx.commit();
            fail("Expecting commit failure");
        } catch (TransactionException exception) {
            assertEquals(Transactions.TX_DELAYED_ACK_ERR, exception.code());
        }

        proc0.setFaultyGroup(null);
        proc1.setFaultyGroup(null);

        assertTrue(tupleView.getAll(null, batch.keySet()).isEmpty());
    }

    @Test
    public void testErrorDuringDirectMappingTwoPartitionTransaction() {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();
        Map<Integer, ClusterNode> mapPartById = map.entrySet().stream().collect(Collectors.toMap(
                entry -> Math.toIntExact(entry.getKey().id()),
                Entry::getValue
        ));

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = unwrapIgniteImpl(server(0));
        IgniteImpl server1 = unwrapIgniteImpl(server(1));

        List<ZonePartitionId> replicationGroupIds = getPartitions(server0);
        ZonePartitionId part0 = replicationGroupIds.get(0);
        ClusterNode firstNode = mapPartById.get(part0.partitionId());

        ZonePartitionId part1 = null;

        // We need to find a partition on other node.
        for (int i = 1; i < replicationGroupIds.size(); i++) {
            ZonePartitionId tmp = replicationGroupIds.get(i);
            ClusterNode otherNode = mapPartById.get(tmp.partitionId());
            if (!otherNode.equals(firstNode)) {
                part1 = tmp;
                break;
            }
        }

        assertNotNull(part1);

        List<Tuple> tuples0 = generateKeysForPartition(300, 1, map, part0.partitionId(), table);
        List<Tuple> tuples1 = generateKeysForPartition(310, 1, map, part1.partitionId(), table);

        FaultyAppendEntriesRequestProcessor proc0 = installFaultyAppendEntriesProcessor(server0);
        proc0.setFaultyGroup(part1);

        FaultyAppendEntriesRequestProcessor proc1 = installFaultyAppendEntriesProcessor(server1);
        proc1.setFaultyGroup(part1);

        KeyValueView<Tuple, Tuple> tupleView = table.keyValueView();

        Transaction tx = client().transactions().begin();

        Map<Tuple, Tuple> batch = new LinkedHashMap<>();

        for (Tuple tup : tuples0) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        for (Tuple tup : tuples1) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        Iterator<Entry<Tuple, Tuple>> iter = batch.entrySet().iterator();
        Entry<Tuple, Tuple> first = iter.next();

        tupleView.put(tx, first.getKey(), first.getValue());

        // Directly mapped request, will cause client inflight failure.
        Entry<Tuple, Tuple> second = iter.next();
        tupleView.put(tx, second.getKey(), second.getValue());

        try {
            tx.commit();
            fail("Expecting commit failure");
        } catch (TransactionException exception) {
            assertEquals(Transactions.TX_DELAYED_ACK_ERR, exception.code());
        }

        proc0.setFaultyGroup(null);
        proc1.setFaultyGroup(null);

        assertTrue(tupleView.getAll(null, batch.keySet()).isEmpty());
    }

    @Test
    public void testErrorDuringDirectMappingTwoPartitionTransactionColocated() {
        Map<Partition, ClusterNode> map = table().partitionDistribution().primaryReplicasAsync().join();
        Map<Integer, ClusterNode> mapPartById = map.entrySet().stream().collect(Collectors.toMap(
                entry -> Math.toIntExact(entry.getKey().id()),
                Entry::getValue
        ));

        ClientTable table = (ClientTable) table();

        IgniteImpl server0 = unwrapIgniteImpl(server(0));
        IgniteImpl server1 = unwrapIgniteImpl(server(1));

        List<ZonePartitionId> replicationGroupIds = getPartitions(server0);
        ZonePartitionId part0 = replicationGroupIds.get(0);
        ClusterNode firstNode = mapPartById.get(part0.partitionId());

        ZonePartitionId part1 = null;

        // We need to find a partition on the same node.
        for (int i = 1; i < replicationGroupIds.size(); i++) {
            ZonePartitionId tmp = replicationGroupIds.get(i);
            ClusterNode otherNode = mapPartById.get(tmp.partitionId());
            if (otherNode.equals(firstNode)) {
                part1 = tmp;
                break;
            }
        }

        assertNotNull(part1);

        List<Tuple> tuples0 = generateKeysForPartition(400, 1, map, part0.partitionId(), table);
        List<Tuple> tuples1 = generateKeysForPartition(410, 1, map, part1.partitionId(), table);

        FaultyAppendEntriesRequestProcessor proc0 = installFaultyAppendEntriesProcessor(server0);
        proc0.setFaultyGroup(part1);

        FaultyAppendEntriesRequestProcessor proc1 = installFaultyAppendEntriesProcessor(server1);
        proc1.setFaultyGroup(part1);

        KeyValueView<Tuple, Tuple> tupleView = table.keyValueView();

        Transaction tx = client().transactions().begin();

        Map<Tuple, Tuple> batch = new LinkedHashMap<>();

        for (Tuple tup : tuples0) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        for (Tuple tup : tuples1) {
            batch.put(tup, val(tup.intValue(0) + ""));
        }

        Iterator<Entry<Tuple, Tuple>> iter = batch.entrySet().iterator();
        Entry<Tuple, Tuple> first = iter.next();

        tupleView.put(tx, first.getKey(), first.getValue());

        // Colocated request, will cause server inflight failure.
        Entry<Tuple, Tuple> second = iter.next();
        tupleView.put(tx, second.getKey(), second.getValue());

        try {
            tx.commit();
            fail("Expecting commit failure");
        } catch (TransactionException exception) {
            assertEquals(Transactions.TX_DELAYED_ACK_ERR, exception.code());
        }

        proc0.setFaultyGroup(null);
        proc1.setFaultyGroup(null);

        assertTrue(tupleView.getAll(null, batch.keySet()).isEmpty());
    }

    private Table table() {
        return client().tables().tables().get(0);
    }

    private static Tuple val(String v) {
        return Tuple.create().set(COLUMN_VAL, v);
    }

    private static Tuple key(Integer k) {
        return Tuple.create().set(COLUMN_KEY, k);
    }

    @Override
    protected int replicas() {
        return 2;
    }

    private static FaultyAppendEntriesRequestProcessor installFaultyAppendEntriesProcessor(IgniteImpl node) {
        RaftServer raftServer = node.raftManager().server();
        RpcServer<?> rpcServer = getFieldValue(raftServer, JraftServerImpl.class, "rpcServer");
        Map<String, RpcProcessor<?>> processors = getFieldValue(rpcServer, IgniteRpcServer.class, "processors");

        AppendEntriesRequestProcessor originalProcessor =
                (AppendEntriesRequestProcessor) processors.get(AppendEntriesRequest.class.getName());
        Executor appenderExecutor = getFieldValue(originalProcessor, RpcRequestProcessor.class, "executor");
        RaftMessagesFactory raftMessagesFactory = getFieldValue(originalProcessor, RpcRequestProcessor.class, "msgFactory");

        FaultyAppendEntriesRequestProcessor blockingProcessor = new FaultyAppendEntriesRequestProcessor(
                appenderExecutor,
                raftMessagesFactory
        );

        rpcServer.registerProcessor(blockingProcessor);

        return blockingProcessor;
    }

    private static class FaultyAppendEntriesRequestProcessor extends AppendEntriesRequestProcessor {
        private volatile @Nullable String partId;

        FaultyAppendEntriesRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
            super(executor, msgFactory);
        }

        @Override
        public Message processRequest0(RaftServerService service, AppendEntriesRequest request, RpcRequestClosure done) {
            boolean isHeartbeat = JRaftUtils.isHeartbeatRequest(request);

            if (partId != null && partId.equals(request.groupId()) && !isHeartbeat) {
                // This response puts replicator to endless retry loop.
                return RaftRpcFactory.DEFAULT //
                        .newResponse(done.getMsgFactory(), RaftError.EINTERNAL,
                                "Fail AppendEntries on '%s'.", request.groupId());
            }

            return super.processRequest0(service, request, done);
        }

        void setFaultyGroup(@Nullable ZonePartitionId partId) {
            this.partId = partId == null ? null : partId.toString();
        }

        @Override
        public String interest() {
            return AppendEntriesRequestImpl.class.getName();
        }
    }

    private static List<ZonePartitionId> getPartitions(IgniteImpl server) {
        Catalog catalog = server.catalogManager().catalog(server.catalogManager().latestCatalogVersion());
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(ZONE_NAME.toUpperCase());
        List<ZonePartitionId> replicationGroupIds = zonePartitionIds(server, zoneDescriptor.id());
        return replicationGroupIds;
    }
}
