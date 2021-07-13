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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDBKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl.DelegatingStateMachine;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Persistent (rocksdb-based) meta storage client tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ITMetaStorageServicePersistenceTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ITMetaStorageServicePersistenceTest.class);

    /** Starting server port. */
    private static final int PORT = 5003;

    /** Starting client port. */
    private static final int CLIENT_PORT = 6003;

    /** */
    @WorkDirectory
    private Path workDir;

    /**
     * Peers list.
     */
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

    /** Servers. */
    private List<JRaftServerImpl> servers = new ArrayList<>();

    /** Clients. */
    private List<RaftGroupService> clients = new ArrayList<>();

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

    /**
     * Test parameters for {@link #testSnapshot}.
     */
    private static class TestData {
        /** Delete raft group folder. */
        private final boolean deleteFolder;

        /** Write to meta storage after a snapshot. */
        private final boolean writeAfterSnapshot;

        /** */
        private TestData(boolean deleteFolder, boolean writeAfterSnapshot) {
            this.deleteFolder = deleteFolder;
            this.writeAfterSnapshot = writeAfterSnapshot;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return String.format("deleteFolder=%s, writeAfterSnapshot=%s", deleteFolder, writeAfterSnapshot);
        }
    }

    /**
     * @return {@link #testSnapshot} parameters.
     */
    private static List<TestData> testSnapshotData() {
        return List.of(
            new TestData(false, false),
            new TestData(true, true),
            new TestData(false, true),
            new TestData(true, false)
        );
    }

    /**
     * Tests that a joining raft node successfuly restores a snapshot.
     *
     * @param testData Test parameters.
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @MethodSource("testSnapshotData")
    public void testSnapshot(TestData testData) throws Exception {
        ByteArray firstKey = ByteArray.fromString("first");
        byte[] firstValue = "firstValue".getBytes();

        RaftGroupService metaStorageSvc = prepareMetaStorage(() ->
            new RocksDBKeyValueStorage(workDir.resolve(UUID.randomUUID().toString()))
        );

        MetaStorageServiceImpl metaStorage = new MetaStorageServiceImpl(metaStorageSvc);

        metaStorage.put(firstKey, firstValue).get();

        Peer leader = metaStorageSvc.leader();

        check(metaStorage, firstKey, firstValue, false, false, 1, 1);

        JRaftServerImpl toStop = null;

        for (JRaftServerImpl server : servers) {
            Peer peer = server.localPeer(METASTORAGE_RAFT_GROUP_NAME);

            if (!peer.equals(leader)) {
                toStop = server;
                break;
            }
        }

        String serverDataPath = toStop.getServerDataPath(METASTORAGE_RAFT_GROUP_NAME);

        int stopIdx = servers.indexOf(toStop);

        servers.remove(stopIdx);

        toStop.shutdown();

        metaStorageSvc.snapshot(metaStorageSvc.leader()).get();

        metaStorage.remove(firstKey).get();

        check(metaStorage, firstKey, null, true, false, 2, 2);

        metaStorage.put(firstKey, firstValue).get();

        check(metaStorage, firstKey, firstValue, false, false, 3, 3);

        metaStorageSvc.snapshot(metaStorageSvc.leader()).get();

        byte[] lastKey = firstKey.bytes();
        byte[] lastValue = firstValue;

        if (testData.deleteFolder)
            IgniteUtils.deleteIfExists(Paths.get(serverDataPath));

        if (testData.writeAfterSnapshot) {
            ByteArray secondKey = ByteArray.fromString("second");
            byte[] secondValue = "secondValue".getBytes();

            metaStorage.put(secondKey, secondValue).get();

            lastKey = secondKey.bytes();
            lastValue = secondValue;
        }

        JRaftServerImpl restarted = startServer(stopIdx, initializer(
            new RocksDBKeyValueStorage(workDir.resolve(UUID.randomUUID().toString()))
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

        MetaStorageServiceImpl metaStorage2 = new MetaStorageServiceImpl(metaStorageSvc);

        int expectedRevision = testData.writeAfterSnapshot ? 4 : 3;
        int expectedUpdateCounter = testData.writeAfterSnapshot ? 4 : 3;

        check(metaStorage2, new ByteArray(lastKey), lastValue, false, false, expectedRevision, expectedUpdateCounter);
    }

    /**
     * Check meta storage entry.
     *
     * @param metaStorage Meta storage service.
     * @param key Key.
     * @param value Expected value.
     * @param isTombstone Expected tombstone flag.
     * @param isEmpty Expected empty flag.
     * @param revision Expected revision.
     * @param updateCounter Expected update counter.
     * @throws ExecutionException If failed.
     * @throws InterruptedException If failed.
     */
    private void check(
        MetaStorageServiceImpl metaStorage,
        ByteArray key,
        byte[] value,
        boolean isTombstone,
        boolean isEmpty,
        int revision,
        int updateCounter
    ) throws ExecutionException, InterruptedException {
        Entry entry = metaStorage.get(key).get();

        assertEquals(isTombstone, entry.tombstone());

        assertEquals(isEmpty, entry.empty());

        assertArrayEquals(
            value,
            entry.value()
        );

        assertEquals(revision, entry.revision());

        assertEquals(updateCounter, entry.updateCounter());
    }

    /** */
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
    private boolean waitForTopology(ClusterService cluster, int exp, int timeout) {
        return waitForCondition(() -> cluster.topologyService().allMembers().size() >= exp, timeout);
    }

    /**
     * @return Local address.
     */
    public static String getLocalAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a cluster service.
     */
    protected ClusterService clusterService(String name, int port, List<NetworkAddress> servers) {
        var context = new ClusterLocalConfiguration(name, port, servers, SERIALIZATION_REGISTRY);

        var network = NETWORK_FACTORY.createClusterService(context);

        network.start();

        cluster.add(network);

        return network;
    }

    /**
     * Starts a raft server.
     *
     * @param idx Server index (affects port of the server).
     * @param clo Server initializer closure.
     * @return Server.
     * @throws IOException If failed.
     */
    private JRaftServerImpl startServer(int idx, Consumer<RaftServer> clo) throws IOException {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = clusterService("server" + idx, PORT + idx, List.of(addr));

        Path jraft = workDir.resolve("jraft" + idx);

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
     * a client.
     *
     * @param keyValStorage Storage.
     * @return Meta storage raft group service instance.
     */
    private RaftGroupService prepareMetaStorage(Supplier<KeyValueStorage> keyValStorage) throws IOException {
        for (int i = 0; i < 3; i++) {
            startServer(i, initializer(keyValStorage.get()));
        }

        return startClient(METASTORAGE_RAFT_GROUP_NAME);
    }

    /**
     * Creates a raft server initializer.
     * @param keyValStorage Key-value storage.
     * @return Initializer.
     */
    private Consumer<RaftServer> initializer(KeyValueStorage keyValStorage) {
        return server -> {
            server.startRaftGroup(
                METASTORAGE_RAFT_GROUP_NAME,
                new MetaStorageListener(keyValStorage),
                INITIAL_CONF
            );
        };
    }

    /**
     * Start a client with the default address.
     */
    private RaftGroupService startClient(String groupId) {
        return startClient(groupId, new NetworkAddress(getLocalAddress(), PORT));
    }

    /**
     * Starts a client with a specific address.
     */
    private RaftGroupService startClient(String groupId, NetworkAddress addr) {
        ClusterService clientNode = clusterService(
            "client_" + groupId + "_", CLIENT_PORT + clients.size(), List.of(addr));

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
