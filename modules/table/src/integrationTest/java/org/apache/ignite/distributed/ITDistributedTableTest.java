/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.distributed;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.RendezvousAffinityFunction;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.basic.ConcurrentHashMapStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.SingleRowResponse;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.LocalPortRangeNodeFinder;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Distributed internal table tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ITDistributedTableTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ITDistributedTableTest.class);

    /** Base network port. */
    public static final int NODE_PORT_BASE = 20_000;

    /** Nodes. */
    private int nodes;

    /** Partitions. */
    public int parts;

    /** Factory. */
    private static final RaftClientMessagesFactory FACTORY = new RaftClientMessagesFactory();

    /** Network factory. */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /** Client. */
    private ClusterService client;

    /** Schema. */
    public static SchemaDescriptor SCHEMA = new SchemaDescriptor(UUID.randomUUID(),
        1,
        new Column[] {new Column("key", NativeTypes.INT64, false)},
        new Column[] {new Column("value", NativeTypes.INT64, false)}
    );

    /** Cluster. */
    private ArrayList<ClusterService> cluster = new ArrayList<>();

    /** */
    private Map<ClusterNode, RaftServer> raftServers;

    /** */
    @WorkDirectory
    private Path dataPath;

    /** */
    private TableImpl table;

    /**
     * Start all cluster nodes before each test.
     * @param memStore {@code True} to start memory store.
     */
    public void startTable(boolean memStore) throws Exception {
        assertTrue(nodes > 0);
        assertTrue(parts > 0);

        var nodeFinder = new LocalPortRangeNodeFinder(NODE_PORT_BASE, NODE_PORT_BASE + nodes);

        nodeFinder.findNodes().stream()
            .map(addr -> startClient(addr.toString(), addr.port(), nodeFinder))
            .forEach(cluster::add);

        for (ClusterService node : cluster)
            assertTrue(waitForTopology(node, nodes, 1000));

        LOG.info("Cluster started.");

        client = startClient("client", NODE_PORT_BASE + nodes, nodeFinder);

        assertTrue(waitForTopology(client, nodes + 1, 1000));

        LOG.info("Client started.");

        raftServers = new HashMap<>(nodes);

        for (int i = 0; i < nodes; i++) {
            TxManagerImpl txManager = new TxManagerImpl(cluster.get(i), new HeapLockManager());

            var raftSrv = new JRaftServerImpl(cluster.get(i), txManager, dataPath);

            raftSrv.start();

            raftServers.put(cluster.get(i).topologyService().localMember(), raftSrv);
        }

        List<List<ClusterNode>> assignment = RendezvousAffinityFunction.assignPartitions(
            cluster.stream().map(node -> node.topologyService().localMember()).collect(Collectors.toList()),
            parts,
            1,
            false,
            null
        );

        int p = 0;

        Map<Integer, RaftGroupService> partMap = new HashMap<>();

        for (List<ClusterNode> partNodes : assignment) {
            RaftServer rs = raftServers.get(partNodes.get(0));

            String grpId = "part-" + p;

            List<Peer> conf = List.of(new Peer(partNodes.get(0).address()));

            assertNotNull(rs.transactionManager());

            rs.startRaftGroup(
                grpId,
                new PartitionListener(new VersionedRowStore(
                    memStore ? new ConcurrentHashMapStorage() : new RocksDbStorage(dataPath.resolve("part" + p),
                        ByteBuffer::compareTo),
                    rs.transactionManager())),
                conf
            );

            RaftGroupService service = RaftGroupServiceImpl.start(grpId, client, FACTORY, 10_000, conf, true, 200)
                .get(3, TimeUnit.SECONDS);

            partMap.put(p, service);

            p++;
        }

        // Originator's tx manager.
        TxManager nearMgr = new TxManagerImpl(client, new HeapLockManager());

        table = new TableImpl(new InternalTableImpl(
            "tbl",
            UUID.randomUUID(),
            partMap,
            parts,
            nearMgr
        ), new SchemaRegistry() {
            @Override public SchemaDescriptor schema() {
                return SCHEMA;
            }

            @Override public int lastSchemaVersion() {
                return SCHEMA.version();
            }

            @Override public SchemaDescriptor schema(int ver) {
                return SCHEMA;
            }

            @Override public Row resolve(BinaryRow row) {
                return new Row(SCHEMA, row);
            }
        }, null, null);
    }

    /**
     * Shutdowns all cluster nodes after each test.
     *
     * @throws Exception If failed.
     */
    @AfterEach
    public void afterTest() throws Exception {
        for (ClusterService node : cluster) {
            node.stop();
        }

        client.stop();
    }

    /**
     * Tests directly partition listener for single client - server configuration.
     *
     * @throws Exception If failed.
     */
//    @Test
//    public void partitionListener() throws Exception {
//        nodes = 1;
//
//        startGrid();
//
//        String grpId = "part";
//
//        ClusterService srv = cluster.get(0);
//
//        TxManagerImpl locMgr = new TxManagerImpl(srv, new HeapLockManager());
//
//        RaftServer partSrv = new JRaftServerImpl(srv, locMgr, dataPath);
//
//        partSrv.start();
//
//        List<Peer> conf = List.of(new Peer(srv.topologyService().localMember().address()));
//
//        assertTrue(partSrv.startRaftGroup(
//            grpId,
//            new PartitionListener(new VersionedRowStore(new RocksDbStorage(dataPath.resolve("db"),
//                ByteBuffer::compareTo), locMgr)),
//            conf
//        ));
//
//        RaftGroupService partRaftGrp =
//            RaftGroupServiceImpl
//                .start(grpId, client, FACTORY, 10_000, conf, true, 200)
//                .get(3, TimeUnit.SECONDS);
//
//        Row testRow = getTestRow();
//
//        TxManagerImpl nearMgr = new TxManagerImpl(client, new HeapLockManager());
//
//        InternalTransaction nearTx = nearMgr.begin();
//
//        assertTrue(partRaftGrp.<Boolean>run(new InsertCommand(testRow, nearTx.timestamp())).join());
//
////        Row keyChunk = new Row(SCHEMA, new ByteBufferRow(testRow.keySlice()));
//        Row keyChunk = getTestKey();
//
//        CompletableFuture<SingleRowResponse> getFut = partRaftGrp.run(new GetCommand(keyChunk, nearTx.timestamp()));
//
//        assertNotNull(getFut.get().getValue());
//
//        assertEquals(testRow.longValue(1), new Row(SCHEMA, getFut.get().getValue()).longValue(1));
//
//        List<NetworkAddress> nodes = nearTx.nodes();
//        assertEquals(2, nodes.size());
//        assertEquals(client.topologyService().localMember().address(), nodes.get(0));
//        assertEquals(srv.topologyService().localMember().address(), nodes.get(1));
//
//        assertEquals(TxState.PENDING, locMgr.state(nearTx.timestamp()));
//
//        nearTx.commit();
//
//        assertEquals(TxState.COMMITED, nearMgr.state(nearTx.timestamp()));
//        assertEquals(TxState.COMMITED, locMgr.state(nearTx.timestamp()));
//
//        partSrv.stop();
//    }

    /**
     * Prepares a test row which contains one field.
     *
     * @return Row.
     */
    @NotNull private Row getTestKey() {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendLong(1L);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }

    /**
     * Prepares a test row which contains two fields.
     *
     * @return Row.
     */
    @NotNull private Row getTestRow() {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendLong(1L);
        rowBuilder.appendLong(10L);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }

    /**
     * Tests tx flow on single client-server nodes topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testComplex() throws Exception {
        nodes = 1;
        parts = 1;

        startTable(true);

        InternalTableImpl impl = (InternalTableImpl) table.internalTable();

        TxManager nearMgr = impl.transactionManager();

        InternalTransaction tx = nearMgr.begin();

        Table txTable = table.withTransaction(tx);

        partitionedTableView(txTable, parts * 10);

        partitionedTableKVBinaryView(txTable.kvView(), parts * 10);

        tx.commit();
    }

    /**
     * Tests tx flow on single client-server nodes topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientServerTopology() throws Exception {
        nodes = 1;
        parts = 1;

        startTable(true);

        TxManager nearMgr = ((InternalTableImpl) table.internalTable()).transactionManager();
        TxManager locMgr = raftServers.get(cluster.get(0).topologyService().localMember()).transactionManager();

        assertNotNull(locMgr);

        assertNotEquals(nearMgr, locMgr);

        InternalTransaction tx = putGet(nearMgr, true);

        assertEquals(TxState.COMMITED, nearMgr.state(tx.timestamp()));
        assertEquals(TxState.COMMITED, locMgr.state(tx.timestamp()));

        long val2 = table.get(makeKey(1)).longValue("value");

        assertEquals(1, val2);
    }

    /**
     * Tests tx flow on single client-server nodes topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientServerTopologyRollback() throws Exception {
        nodes = 1;
        parts = 1;

        startTable(true);

        TxManager nearMgr = ((InternalTableImpl) table.internalTable()).transactionManager();
        TxManager locMgr = raftServers.get(cluster.get(0).topologyService().localMember()).transactionManager();

        InternalTransaction tx = putGet(nearMgr, false);

        assertEquals(TxState.ABORTED, nearMgr.state(tx.timestamp()));
        assertEquals(TxState.ABORTED, locMgr.state(tx.timestamp()));

        assertNull(table.get(makeKey(1)));
    }

    /**
     * @param nearMgr Near TX manager.
     * @param commit {@code True} to commit.
     * @throws TransactionException
     */
    private InternalTransaction putGet(TxManager nearMgr, boolean commit) throws TransactionException {
        InternalTransaction tx = nearMgr.begin();

        assertEquals(1, tx.nodes().size());

        Table txTable = table.withTransaction(tx);

        txTable.upsert(makeValue(1, 1));

        long val = txTable.get(makeKey(1)).longValue("value");

        assertEquals(1, val);

        assertEquals(2, tx.nodes().size());

        if (commit)
            tx.commit();
        else
            tx.rollback();

        return tx;
    }

    /**
     * The test prepares a distributed table and checks operation over various views.
     *
     * @throws Exception If failed.
     */
    @Test
    public void partitionedTable() throws Exception {
        nodes = 1;
        parts = 1;

//        InternalTransaction tx = nearMgr.begin();
//
//        Table txTable = tbl.withTransaction(tx);
//
//        partitionedTableView(txTable, PARTS * 10);
//
//        partitionedTableKVBinaryView(txTable.kvView(), PARTS * 10);
//
//        tx.commit();
    }

    /**
     * Checks operation over row table view.
     *
     * @param view Table view.
     * @param keysCnt Count of keys.
     */
    public void partitionedTableView(Table view, int keysCnt) {
        LOG.info("Test for Table view [keys={}]", keysCnt);

        for (int i = 0; i < keysCnt; i++) {
            view.insert(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .set("value", Long.valueOf(i + 2))
                .build()
            );
        }

        for (int i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertEquals(Long.valueOf(i + 2), entry.longValue("value"));
        }

        for (int i = 0; i < keysCnt; i++) {
            view.upsert(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .set("value", Long.valueOf(i + 5))
                .build()
            );

            Tuple entry = view.get(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertEquals(Long.valueOf(i + 5), entry.longValue("value"));
        }

        HashSet<Tuple> keys = new HashSet<>();

        for (int i = 0; i < keysCnt; i++) {
            keys.add(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());
        }

        Collection<Tuple> entries = view.getAll(keys);

        assertEquals(keysCnt, entries.size());

        for (int i = 0; i < keysCnt; i++) {
            boolean res = view.replace(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .set("value", Long.valueOf(i + 5))
                    .build(),
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .set("value", Long.valueOf(i + 2))
                    .build());

            assertTrue(res);
        }

        for (int i = 0; i < keysCnt; i++) {
            boolean res = view.delete(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertTrue(res);

            Tuple entry = view.get(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertNull(entry);
        }

        ArrayList<Tuple> batch = new ArrayList<>(keysCnt);

        for (int i = 0; i < keysCnt; i++) {
            batch.add(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .set("value", Long.valueOf(i + 2))
                .build());
        }

        view.upsertAll(batch);

        for (int i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertEquals(Long.valueOf(i + 2), entry.longValue("value"));
        }

        view.deleteAll(keys);

        for (Tuple key : keys) {
            Tuple entry = view.get(key);

            assertNull(entry);
        }
    }

    /**
     * Checks operation over key-value binary table view.
     *
     * @param view Table view.
     * @param keysCnt Count of keys.
     */
    public void partitionedTableKVBinaryView(KeyValueBinaryView view, int keysCnt) {
        LOG.info("Tes for Key-Value binary view [keys={}]", keysCnt);

        for (int i = 0; i < keysCnt; i++) {
            view.putIfAbsent(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build(),
                view.tupleBuilder()
                    .set("value", Long.valueOf(i + 2))
                    .build());
        }

        for (int i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build());

            assertEquals(Long.valueOf(i + 2), entry.longValue("value"));
        }

        for (int i = 0; i < keysCnt; i++) {
            view.put(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build(),
                view.tupleBuilder()
                    .set("value", Long.valueOf(i + 5))
                    .build());

            Tuple entry = view.get(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build());

            assertEquals(Long.valueOf(i + 5), entry.longValue("value"));
        }

        HashSet<Tuple> keys = new HashSet<>();

        for (int i = 0; i < keysCnt; i++) {
            keys.add(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());
        }

        Map<Tuple, Tuple> entries = view.getAll(keys);

        assertEquals(keysCnt, entries.size());

        for (int i = 0; i < keysCnt; i++) {
            boolean res = view.replace(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build(),
                view.tupleBuilder()
                    .set("value", Long.valueOf(i + 5))
                    .build(),
                view.tupleBuilder()
                    .set("value", Long.valueOf(i + 2))
                    .build());

            assertTrue(res);
        }

        for (int i = 0; i < keysCnt; i++) {
            boolean res = view.remove(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build());

            assertTrue(res);

            Tuple entry = view.get(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build());

            assertNull(entry);
        }

        HashMap<Tuple, Tuple> batch = new HashMap<>(keysCnt);

        for (int i = 0; i < keysCnt; i++) {
            batch.put(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build(),
                view.tupleBuilder()
                    .set("value", Long.valueOf(i + 2))
                    .build());
        }

        view.putAll(batch);

        for (int i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertEquals(Long.valueOf(i + 2), entry.longValue("value"));
        }

        view.removeAll(keys);

        for (Tuple key : keys) {
            Tuple entry = view.get(key);

            assertNull(entry);
        }
    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param nodeFinder Node finder.
     * @return The client cluster view.
     */
    private static ClusterService startClient(String name, int port, NodeFinder nodeFinder) {
        var network = ClusterServiceTestUtils.clusterService(
            name,
            port,
            nodeFinder,
            SERIALIZATION_REGISTRY,
            NETWORK_FACTORY
        );

        network.start();

        return network;
    }

    /**
     * @param cluster The cluster.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    private boolean waitForTopology(ClusterService cluster, int expected, int timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cluster.topologyService().allMembers().size() >= expected)
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }



    /**
     * @param id The id.
     * @return The key tuple.
     */
    private Tuple makeKey(long id) {
        return table.tupleBuilder().set("key", id).build();
    }

    /**
     * @param id The id.
     * @param balance The balance.
     * @return The value tuple.
     */
    private Tuple makeValue(long id, long val) {
        return table.tupleBuilder().set("key", id).set("value", val).build();
    }
}
