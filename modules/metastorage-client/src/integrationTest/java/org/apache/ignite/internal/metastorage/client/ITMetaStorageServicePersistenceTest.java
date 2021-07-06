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
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.RocksDBKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl.DelegatingStateMachine;
import org.apache.ignite.lang.ByteArray;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Meta storage client tests.
 */
@SuppressWarnings("WeakerAccess")
public class ITMetaStorageServicePersistenceTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ITMetaStorageServicePersistenceTest.class);

    private static final int PORT = 5003;

    private static final int CLIENT_PORT = 6003;

    private static final List<Peer> INITIAL_CONF = IntStream.rangeClosed(0, 2)
        .mapToObj(i -> new NetworkAddress(getLocalAddress(), PORT + i))
        .map(Peer::new)
        .collect(Collectors.toUnmodifiableList());

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

    /** Cluster. */
    private ArrayList<ClusterService> cluster = new ArrayList<>();

    private List<JRaftServerImpl> servers = new ArrayList<>();

    private List<RaftGroupService> clients = new ArrayList<>();

    /**
     * Run {@code NODES} cluster nodes.
     */
    @BeforeEach
    public void beforeTest() {
    }

    /**
     * Shutdown raft server and stop all cluster nodes.
     *
     * @throws Exception If failed to shutdown raft server,
     */
    @AfterEach
    public void afterTest() throws Exception {
        for (RaftGroupService client : clients)
            client.shutdown();

        for (JRaftServerImpl server : servers)
            server.shutdown();
    }

    private static class TestData {
        private final boolean deleteFolder;

        private final boolean writeAfterSnapshot;

        private TestData(boolean deleteFolder, boolean writeAfterSnapshot) {
            this.deleteFolder = deleteFolder;
            this.writeAfterSnapshot = writeAfterSnapshot;
        }

        @Override public String toString() {
            return String.format("deleteFolder=%s, writeAfterSnapshot=%s", deleteFolder, writeAfterSnapshot);
        }
    }

    private static List<TestData> testSnapshotData() {
        return List.of(
            new TestData(false, false),
            new TestData(true, true),
            new TestData(false, true),
            new TestData(true, false)
        );
    }

    @ParameterizedTest
    @MethodSource("testSnapshotData")
    public void testSnapshot(TestData testData) throws Exception {
        ByteArray firstKey = ByteArray.fromString("first");
        byte[] firstValue = "firstValue".getBytes();

        RaftGroupService metaStorageSvc = prepareMetaStorage(
            () -> {
                try {
                    return new RocksDBKeyValueStorage(Files.createTempDirectory("1"));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        );

        MetaStorageServiceImpl metaStorage = new MetaStorageServiceImpl(metaStorageSvc);

        metaStorage.put(firstKey, firstValue).get();

        Peer leader = metaStorageSvc.leader();

        Entry get1 = metaStorage.get(firstKey).get();

        assertArrayEquals(
            firstValue,
            get1.value()
        );

        assertEquals(1, get1.revision());
        assertEquals(1, get1.updateCounter());

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

        metaStorage.remove(firstKey).get();

        Entry get2 = metaStorage.get(firstKey).get();

        assertTrue(get2.tombstone());
        assertFalse(get2.empty());
        assertNull(get2.value());
        assertEquals(2, get2.revision());
        assertEquals(2, get2.updateCounter());

        metaStorage.put(firstKey, firstValue).get();

        Entry get3 = metaStorage.get(firstKey).get();

        assertFalse(get3.tombstone());
        assertFalse(get3.empty());
        assertArrayEquals(
            firstValue,
            get3.value()
        );
        assertEquals(3, get3.revision());
        assertEquals(3, get3.updateCounter());

        metaStorageSvc.snapshot(metaStorageSvc.leader()).get();

        byte[] lastKey = firstKey.bytes();
        byte[] lastValue = firstValue;

        if (testData.deleteFolder)
            Utils.delete(new File(serverDataPath0));

        if (testData.writeAfterSnapshot) {
            ByteArray secondKey = ByteArray.fromString("second");
            byte[] secondValue = "secondValue".getBytes();

            metaStorage.put(secondKey, secondValue).get();

            lastKey = secondKey.bytes();
            lastValue = secondValue;
        }

        JRaftServerImpl restarted = startServer(stopIdx, initializer(
            new RocksDBKeyValueStorage(Files.createTempDirectory("2"))
        ));

        org.apache.ignite.raft.jraft.RaftGroupService svc = restarted.raftGroupService(METASTORAGE_RAFT_GROUP_NAME);

        DelegatingStateMachine fsm = (DelegatingStateMachine) svc.getRaftNode().getOptions().getFsm();

        MetaStorageListener listener = (MetaStorageListener) fsm.getListener();

        byte[] finalLastValue = lastValue;
        byte[] finalLastKey = lastKey;

        boolean success = waitForCondition(() -> {
            KeyValueStorage storage = listener.getStorage();
            org.apache.ignite.internal.metastorage.server.Entry e = storage.get(finalLastKey);
            return !e.empty() && Arrays.equals(finalLastValue, e.value());
        }, 10_000);

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
        return waitForCondition(() -> cluster.topologyService().allMembers().size() >= exp, timeout);
    }

    public static String getLocalAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
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

    private JRaftServerImpl startServer(int idx, Consumer<RaftServer> clo) throws IOException {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = clusterService("server" + idx, PORT + idx, List.of(addr), true);

        Path jraft = Files.createTempDirectory("jraft");

        JRaftServerImpl server = new JRaftServerImpl(service, jraft.toString(), FACTORY) {
            @Override public void shutdown() throws Exception {
                super.shutdown();

                service.shutdown();
            }
        };

        clo.accept(server);

        servers.add(server);

        assertTrue(waitForTopology(service, servers.size(), 3_000));

        return server;
    }

    /**
     * Prepares meta storage by instantiating corresponding raft server with {@link MetaStorageListener} and
     * {@link MetaStorageServiceImpl}.
     *
     * @param keyValStorageMock {@link KeyValueStorage} mock.
     * @return {@link MetaStorageService} instance.
     */
    private RaftGroupService prepareMetaStorage(Supplier<KeyValueStorage> keyValStorage) throws IOException {
        for (int i = 0; i < 3; i++) {
            startServer(i, initializer(keyValStorage.get()));
        }

        return startClient(METASTORAGE_RAFT_GROUP_NAME);
    }

    private Consumer<RaftServer> initializer(KeyValueStorage keyValStorageMock) {
        return server -> {
            server.startRaftGroup(
                METASTORAGE_RAFT_GROUP_NAME,
                new MetaStorageListener(keyValStorageMock),
                INITIAL_CONF
            );
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
}
