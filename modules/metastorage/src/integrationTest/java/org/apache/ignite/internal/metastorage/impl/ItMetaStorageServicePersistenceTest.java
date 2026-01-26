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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.TestMetasStorageUtils;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.ItAbstractListenerSnapshotTest;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Persistent (rocksdb-based) meta storage raft group snapshots tests.
 */
@ExtendWith(ExecutorServiceExtension.class)
public class ItMetaStorageServicePersistenceTest extends ItAbstractListenerSnapshotTest<MetaStorageListener> {
    private static final ByteArray FIRST_KEY = ByteArray.fromString("first");

    private static final byte[] FIRST_VALUE = "firstValue".getBytes(StandardCharsets.UTF_8);

    private static final ByteArray SECOND_KEY = ByteArray.fromString("second");

    private static final byte[] SECOND_VALUE = "secondValue".getBytes(StandardCharsets.UTF_8);

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutorService;

    private MetaStorageServiceImpl metaStorage;

    private final Map<String, RocksDbKeyValueStorage> storageByName = new HashMap<>();

    @Override
    @AfterEach
    public void afterTest() throws Exception {
        super.afterTest();

        IgniteUtils.closeAll(storageByName.values().stream().map(storage -> storage::close));
    }

    @Override
    public void beforeFollowerStop(RaftGroupService service, RaftServer server) {
        InternalClusterNode followerNode = getNode(server);

        metaStorage = new MetaStorageServiceImpl(
                followerNode.name(),
                service,
                new IgniteSpinBusyLock(),
                server.options().getClock(),
                followerNode.id()
        );

        // Put some data in the metastorage
        assertThat(metaStorage.put(FIRST_KEY, FIRST_VALUE), willCompleteSuccessfully());

        // Check that data has been written successfully
        checkEntry(FIRST_KEY.bytes(), FIRST_VALUE, 1);
    }

    @Override
    public void afterFollowerStop(RaftGroupService service, RaftServer server, int stoppedNodeIndex) throws Exception {
        InternalClusterNode followerNode = getNode(server);

        KeyValueStorage storage = storageByName.remove(followerNode.name());

        if (storage != null) {
            storage.close();
        }

        // Remove the first key from the metastorage
        assertThat(metaStorage.remove(FIRST_KEY), willCompleteSuccessfully());

        // Check that data has been removed
        checkEntry(FIRST_KEY.bytes(), null, 2);

        // Put same data again
        assertThat(metaStorage.put(FIRST_KEY, FIRST_VALUE), willCompleteSuccessfully());

        // Check that it has been written
        checkEntry(FIRST_KEY.bytes(), FIRST_VALUE, 3);
    }

    @Override
    public void afterSnapshot(RaftGroupService service) {
        assertThat(metaStorage.put(SECOND_KEY, SECOND_VALUE), willCompleteSuccessfully());
    }

    @Override
    public BooleanSupplier snapshotCheckClosure(JraftServerImpl restarted, boolean interactedAfterSnapshot) {
        InternalClusterNode node = getNode(restarted);

        KeyValueStorage storage = storageByName.get(node.name());

        int expectedRevision = interactedAfterSnapshot ? 4 : 3;

        return () -> storage.revision() == expectedRevision;
    }

    @Override
    public Path getListenerPersistencePath(MetaStorageListener listener, RaftServer server) {
        return storageByName.get(getNode(server).name()).getDbPath();
    }

    @Override
    public RaftGroupListener createListener(ClusterService service, RaftServer server, Path listenerPersistencePath) {
        String nodeName = service.nodeName();

        KeyValueStorage storage = storageByName.computeIfAbsent(nodeName, name -> {
            var s = new RocksDbKeyValueStorage(
                    name,
                    listenerPersistencePath,
                    new NoOpFailureManager(),
                    new ReadOperationForCompactionTracker(),
                    scheduledExecutorService
            );

            s.start();

            return s;
        });

        HybridClock clock = server.options().getClock();
        return new MetaStorageListener(storage, clock, new ClusterTimeImpl(nodeName, new IgniteSpinBusyLock(), clock));
    }

    @Override
    public TestReplicationGroupId raftGroupId() {
        return new TestReplicationGroupId("metastorage");
    }

    @Override
    protected Marshaller commandsMarshaller(ClusterService clusterService) {
        return new ThreadLocalOptimizedMarshaller(clusterService.serializationRegistry());
    }

    private void checkEntry(byte[] expKey, byte @Nullable [] expValue, long expRevision) {
        CompletableFuture<Entry> future = metaStorage.get(new ByteArray(expKey));

        assertThat(future, willCompleteSuccessfully());

        TestMetasStorageUtils.checkEntry(future.join(), expKey, expValue, expRevision);
    }

    private static InternalClusterNode getNode(RaftServer server) {
        return server.clusterService().topologyService().localMember();
    }
}
