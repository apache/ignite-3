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

package org.apache.ignite.internal.metastorage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.ItAbstractListenerSnapshotTest;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.AfterEach;

/**
 * Persistent (rocksdb-based) meta storage raft group snapshots tests.
 */
public class ItMetaStorageServicePersistenceTest extends ItAbstractListenerSnapshotTest<MetaStorageListener> {
    private static final ByteArray FIRST_KEY = ByteArray.fromString("first");

    private static final byte[] FIRST_VALUE = "firstValue".getBytes(StandardCharsets.UTF_8);

    private static final ByteArray SECOND_KEY = ByteArray.fromString("second");

    private static final byte[] SECOND_VALUE = "secondValue".getBytes(StandardCharsets.UTF_8);

    private MetaStorageServiceImpl metaStorage;

    private final Map<String, RocksDbKeyValueStorage> storageByName = new HashMap<>();

    /** After each. */
    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(storageByName.values().stream().map(storage -> storage::close));
    }

    /** {@inheritDoc} */
    @Override
    public void beforeFollowerStop(RaftGroupService service, RaftServer server) throws Exception {
        ClusterNode followerNode = getNode(server);

        var clusterTime = new ClusterTimeImpl(followerNode.name(), new IgniteSpinBusyLock(), new HybridClockImpl());

        metaStorage = new MetaStorageServiceImpl(
                followerNode.name(),
                service,
                new IgniteSpinBusyLock(),
                clusterTime,
                followerNode::id
        );

        // Put some data in the metastorage
        metaStorage.put(FIRST_KEY, FIRST_VALUE).get();

        // Check that data has been written successfully
        check(metaStorage, new EntryImpl(FIRST_KEY.bytes(), FIRST_VALUE, 1, 1));
    }

    /** {@inheritDoc} */
    @Override
    public void afterFollowerStop(RaftGroupService service, RaftServer server, int stoppedNodeIndex) throws Exception {
        ClusterNode followerNode = getNode(server);

        KeyValueStorage storage = storageByName.remove(followerNode.name());

        if (storage != null) {
            storage.close();
        }

        // Remove the first key from the metastorage
        metaStorage.remove(FIRST_KEY).get();

        // Check that data has been removed
        check(metaStorage, new EntryImpl(FIRST_KEY.bytes(), null, 2, 2));

        // Put same data again
        metaStorage.put(FIRST_KEY, FIRST_VALUE).get();

        // Check that it has been written
        check(metaStorage, new EntryImpl(FIRST_KEY.bytes(), FIRST_VALUE, 3, 3));
    }

    /** {@inheritDoc} */
    @Override
    public void afterSnapshot(RaftGroupService service) throws Exception {
        metaStorage.put(SECOND_KEY, SECOND_VALUE).get();
    }

    /** {@inheritDoc} */
    @Override
    public BooleanSupplier snapshotCheckClosure(JraftServerImpl restarted, boolean interactedAfterSnapshot) {
        ClusterNode node = getNode(restarted);

        KeyValueStorage storage = storageByName.get(node.name());

        byte[] lastKey = interactedAfterSnapshot ? SECOND_KEY.bytes() : FIRST_KEY.bytes();
        byte[] lastValue = interactedAfterSnapshot ? SECOND_VALUE : FIRST_VALUE;

        int expectedRevision = interactedAfterSnapshot ? 4 : 3;
        int expectedUpdateCounter = interactedAfterSnapshot ? 4 : 3;

        EntryImpl expectedLastEntry = new EntryImpl(lastKey, lastValue, expectedRevision, expectedUpdateCounter);

        return () -> storage.get(lastKey).equals(expectedLastEntry);
    }

    /** {@inheritDoc} */
    @Override
    public Path getListenerPersistencePath(MetaStorageListener listener, RaftServer server) {
        return storageByName.get(getNode(server).name()).getDbPath();
    }

    /** {@inheritDoc} */
    @Override
    public RaftGroupListener createListener(ClusterService service, Path listenerPersistencePath) {
        String nodeName = service.nodeName();

        KeyValueStorage storage = storageByName.computeIfAbsent(nodeName, name -> {
            var s = new RocksDbKeyValueStorage(name, listenerPersistencePath, new NoOpFailureProcessor());

            s.start();

            return s;
        });

        return new MetaStorageListener(storage, new ClusterTimeImpl(nodeName, new IgniteSpinBusyLock(), new HybridClockImpl()));
    }

    /** {@inheritDoc} */
    @Override
    public TestReplicationGroupId raftGroupId() {
        return new TestReplicationGroupId("metastorage");
    }

    @Override
    protected Marshaller commandsMarshaller(ClusterService clusterService) {
        return new ThreadLocalOptimizedMarshaller(clusterService.serializationRegistry());
    }

    /**
     * Check meta storage entry.
     *
     * @param metaStorage Meta storage service.
     * @param expected    Expected entry.
     * @throws ExecutionException   If failed.
     * @throws InterruptedException If failed.
     */
    private static void check(MetaStorageServiceImpl metaStorage, EntryImpl expected)
            throws ExecutionException, InterruptedException {
        Entry entry = metaStorage.get(new ByteArray(expected.key())).get();

        assertEquals(expected, entry);
    }

    private static ClusterNode getNode(RaftServer server) {
        return server.clusterService().topologyService().localMember();
    }
}
