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

package org.apache.ignite.internal.metastorage.client;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.metastorage.common.command.GetCommand;
import org.apache.ignite.internal.metastorage.common.command.SingleEntryResponse;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.RocksDBKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.jraft.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Meta storage client tests.
 */
@SuppressWarnings("WeakerAccess")
public class ITMetaStorageServicePersistenceTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ITMetaStorageServicePersistenceTest.class);

    private static final int PORT = 5003;

    private static final int CLIENT_PORT = 6003;

    /** Base network port. */
    private static final int NODE_PORT_BASE = 20_000;

    private static final List<Peer> INITIAL_CONF = IntStream.rangeClosed(0, 2)
        .mapToObj(i -> new NetworkAddress(getLocalAddress(), PORT + i))
        .map(Peer::new)
        .collect(Collectors.toUnmodifiableList());

    /** Nodes. */
    private static final int NODES = 3;

    /** */
    private static final String METASTORAGE_RAFT_GROUP_NAME = "METASTORAGE_RAFT_GROUP";

    /** */
    public static final int LATEST_REVISION = -1;

    /** Factory. */
    private static final RaftClientMessagesFactory FACTORY = new RaftClientMessagesFactory();

    /** Network factory. */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /**  Expected server result entry. */
    private static final org.apache.ignite.internal.metastorage.server.Entry EXPECTED_SRV_RESULT_ENTRY =
        new org.apache.ignite.internal.metastorage.server.Entry(
            new byte[] {1},
            new byte[] {2},
            10,
            2
        );

    /**  Expected server result entry. */
    private static final EntryImpl EXPECTED_RESULT_ENTRY =
        new EntryImpl(
            new ByteArray(new byte[] {1}),
            new byte[] {2},
            10,
            2
        );

    /** Expected result map. */
    private static final NavigableMap<ByteArray, Entry> EXPECTED_RESULT_MAP;

    private static final Collection<org.apache.ignite.internal.metastorage.server.Entry> EXPECTED_SRV_RESULT_COLL;

    /** Cluster. */
    private ArrayList<ClusterService> cluster = new ArrayList<>();

    /**  Meta storage raft server. */
    private RaftServer metaStorageRaftSrv;

    static {
        EXPECTED_RESULT_MAP = new TreeMap<>();

        EntryImpl entry1 = new EntryImpl(
                new ByteArray(new byte[]{1}),
                new byte[]{2},
                10,
                2
        );

        EXPECTED_RESULT_MAP.put(entry1.key(), entry1);

        EntryImpl entry2 = new EntryImpl(
                new ByteArray(new byte[]{3}),
                new byte[]{4},
                10,
                3
        );

        EXPECTED_RESULT_MAP.put(entry2.key(), entry2);

        EXPECTED_SRV_RESULT_COLL = new ArrayList<>();

        EXPECTED_SRV_RESULT_COLL.add(new org.apache.ignite.internal.metastorage.server.Entry(
                entry1.key().bytes(), entry1.value(), entry1.revision(), entry1.updateCounter()
        ));

        EXPECTED_SRV_RESULT_COLL.add(new org.apache.ignite.internal.metastorage.server.Entry(
                entry2.key().bytes(), entry2.value(), entry2.revision(), entry2.updateCounter()
        ));
    }

    private List<JRaftServerImpl> servers = new ArrayList<>();
    private List<RaftGroupService> clients = new ArrayList<>();

    /**
     * Run {@code NODES} cluster nodes.
     */
    @BeforeEach
    public void beforeTest() {
//        List<NetworkAddress> servers = IntStream.range(NODE_PORT_BASE, NODE_PORT_BASE + NODES)
//            .mapToObj(port -> new NetworkAddress("localhost", port))
//            .collect(Collectors.toList());
//
//        for (int i = 0; i < NODES; i++)
//            cluster.add(startClusterNode("node_" + i, NODE_PORT_BASE + i, servers));
//
//        for (ClusterService node : cluster)
//            assertTrue(waitForTopology(node, NODES, 1000));
//
//        LOG.info("Cluster started.");
    }

    /**
     * Shutdown raft server and stop all cluster nodes.
     *
     * @throws Exception If failed to shutdown raft server,
     */
    @AfterEach
    public void afterTest() throws Exception {
//        metaStorageRaftSrv.shutdown();
//
//        for (ClusterService node : cluster)
//            node.shutdown();
    }

    @Test
    public void testSnapshot() throws Exception {
        byte[] expVal = new byte[]{2};

        RaftGroupService metaStorageSvc = prepareMetaStorage(new RocksDBKeyValueStorage());

        MetaStorageServiceImpl metaStorage = new MetaStorageServiceImpl(metaStorageSvc);

        metaStorage.put(EXPECTED_RESULT_ENTRY.key(), expVal).get();

        Peer leader = metaStorageSvc.leader();

        assertArrayEquals(
            EXPECTED_RESULT_ENTRY.value(),
            metaStorage.get(EXPECTED_RESULT_ENTRY.key()).get().value()
        );

        JRaftServerImpl toStop = null;

        for (JRaftServerImpl server : servers) {
            Peer peer = server.localPeer(METASTORAGE_RAFT_GROUP_NAME);

            if (!peer.equals(leader)) {
                toStop = server;
                break;
            }
        }

        String serverDataPath0 = toStop.getServerDataPath(METASTORAGE_RAFT_GROUP_NAME);

        int stopIdx = servers.indexOf(toStop);

        servers.remove(stopIdx);

        toStop.shutdown();

        metaStorageSvc.snapshot(metaStorageSvc.leader()).get();

        metaStorage.remove(EXPECTED_RESULT_ENTRY.key()).get();

        metaStorage.put(EXPECTED_RESULT_ENTRY.key(), expVal).get();

        metaStorageSvc.snapshot(metaStorageSvc.leader()).get();

        Utils.delete(new File(serverDataPath0));

        JRaftServerImpl restarted = startServer(stopIdx, initializer(new RocksDBKeyValueStorage()));

        RaftGroupService svc2 = startClient(METASTORAGE_RAFT_GROUP_NAME, restarted.clusterService().topologyService().localMember().address());

        SingleEntryResponse entry = svc2.<SingleEntryResponse>run(new GetCommand(EXPECTED_RESULT_ENTRY.key())).get();

        org.apache.ignite.raft.jraft.RaftGroupService svc = restarted.raftGroupService(METASTORAGE_RAFT_GROUP_NAME);

        JRaftServerImpl.DelegatingStateMachine fsm0 =
            (JRaftServerImpl.DelegatingStateMachine) svc.getRaftNode().getOptions().getFsm();

        MetaStorageListener listener = (MetaStorageListener) fsm0.getListener();

        System.out.println("");
        boolean success = waitForCondition(() -> {
            KeyValueStorage storage = listener.getStorage();
            return !storage.get(EXPECTED_RESULT_ENTRY.key().bytes()).empty();
        }, 150_000);

        assertTrue(success);
    }

    @SuppressWarnings("BusyWait") public static boolean waitForCondition(BooleanSupplier cond, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cond.getAsBoolean())
                return true;

            try {
                sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }

    /**
     * @param cluster The cluster.
     * @param exp Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    @SuppressWarnings("SameParameterValue")
    private boolean waitForTopology(ClusterService cluster, int exp, int timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cluster.topologyService().allMembers().size() >= exp)
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

    public static String getLocalAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException e) {
            throw new IgniteInternalException(e);
        }
    }

    protected ClusterService clusterService(String name, int port, List<NetworkAddress> servers, boolean start) {
        var context = new ClusterLocalConfiguration(name, port, servers, SERIALIZATION_REGISTRY);

        var network = NETWORK_FACTORY.createClusterService(context);

        if (start)
            network.start();

        cluster.add(network);

        return network;
    }

    private JRaftServerImpl startServer(int idx, Consumer<RaftServer> clo) {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = clusterService("server" + idx, PORT + idx, List.of(addr), true);

        Path jraft = null;
        try {
            jraft = Files.createTempDirectory("jraft");
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        JRaftServerImpl server = new JRaftServerImpl(service, jraft.toString(), FACTORY) {
            @Override public void shutdown() throws Exception {
                super.shutdown();

                service.shutdown();
            }
        };

        clo.accept(server);

        servers.add(server);

        assertTrue(waitForTopology(service, servers.size(), 15_000));

        return server;
    }

    /**
     * Prepares meta storage by instantiating corresponding raft server with {@link MetaStorageListener} and
     * {@link MetaStorageServiceImpl}.
     *
     * @param keyValStorageMock {@link KeyValueStorage} mock.
     * @return {@link MetaStorageService} instance.
     */
    private RaftGroupService prepareMetaStorage(KeyValueStorage keyValStorageMock) {
        for (int i = 0; i < 3; i++) {
            startServer(i, initializer(keyValStorageMock));
        }

        return startClient(METASTORAGE_RAFT_GROUP_NAME);
    }

    private Consumer<RaftServer> initializer(KeyValueStorage keyValStorageMock) {
        return server -> {
            try {
                Path rocksdb = Files.createTempDirectory("rocksdb");
                server.
                    startRaftGroup(
                        METASTORAGE_RAFT_GROUP_NAME,
                        new MetaStorageListener(keyValStorageMock),
                        INITIAL_CONF
                    );
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        };
    }

    private RaftGroupService startClient(String groupId) {
        return startClient(groupId, new NetworkAddress(getLocalAddress(), PORT));
    }

    private RaftGroupService startClient(String groupId, NetworkAddress addr) {
        ClusterService clientNode = clusterService(
            "client_" + groupId + "_", CLIENT_PORT + clients.size(), List.of(addr), true);

        RaftGroupServiceImpl client = new RaftGroupServiceImpl(groupId, clientNode, FACTORY, 10_000,
            List.of(new Peer(addr)), false, 200) {
            @Override public void shutdown() {
                super.shutdown();

                clientNode.shutdown();
            }
        };

        clients.add(client);

        return client;
    }

    /**
     * Abstract {@link KeyValueStorage}. Used for tests purposes.
     */
    @SuppressWarnings("JavaAbbreviationUsage")
    private abstract class AbstractKeyValueStorage implements KeyValueStorage {
        /** {@inheritDoc} */
        @Override public long revision() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long updateCounter() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry get(byte[] key) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry get(byte[] key, long rev) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public @NotNull Collection<org.apache.ignite.internal.metastorage.server.Entry> getAll(List<byte[]> keys) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public @NotNull Collection<org.apache.ignite.internal.metastorage.server.Entry> getAll(List<byte[]> keys, long revUpperBound) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(byte[] key, byte[] value) {
            fail();
        }

        /** {@inheritDoc} */
        @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry getAndPut(byte[] key, byte[] value) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void putAll(List<byte[]> keys, List<byte[]> values) {
            fail();
        }

        /** {@inheritDoc} */
        @Override public @NotNull Collection<org.apache.ignite.internal.metastorage.server.Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void remove(byte[] key) {
            fail();
        }

        /** {@inheritDoc} */
        @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry getAndRemove(byte[] key) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void removeAll(List<byte[]> keys) {
            fail();
        }

        /** {@inheritDoc} */
        @Override public @NotNull Collection<org.apache.ignite.internal.metastorage.server.Entry> getAndRemoveAll(List<byte[]> keys) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean invoke(
                org.apache.ignite.internal.metastorage.server.Condition condition,
                Collection<org.apache.ignite.internal.metastorage.server.Operation> success,
                Collection<org.apache.ignite.internal.metastorage.server.Operation> failure
        ) {
            fail();

            return false;
        }

        /** {@inheritDoc} */
        @Override public Cursor<org.apache.ignite.internal.metastorage.server.Entry> range(byte[] keyFrom, byte[] keyTo) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Cursor<org.apache.ignite.internal.metastorage.server.Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Cursor<org.apache.ignite.internal.metastorage.server.WatchEvent> watch(byte[] keyFrom, byte[] keyTo, long rev) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Cursor<org.apache.ignite.internal.metastorage.server.WatchEvent> watch(byte[] key, long rev) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Cursor<org.apache.ignite.internal.metastorage.server.WatchEvent> watch(Collection<byte[]> keys, long rev) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void compact() {
            fail();
        }

        /** {@inheritDoc} */
        @Override public void close() {
            fail();
        }

        /** {@inheritDoc} */
        @Override public CompletableFuture<Void> snapshot(Path snapshotPath) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void restoreSnapshot(Path snapshotPath) {
            fail();
        }
    }
}
