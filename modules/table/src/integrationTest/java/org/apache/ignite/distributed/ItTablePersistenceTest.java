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

package org.apache.ignite.distributed;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestConcurrentHashMapTxStateStorage;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.service.ItAbstractListenerSnapshotTest;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;

/**
 * Persistent partitions raft group snapshots tests.
 */
@Disabled("IGNITE-16644, IGNITE-17817 MvPartitionStorage hasn't supported snapshots yet")
public class ItTablePersistenceTest extends ItAbstractListenerSnapshotTest<PartitionListener> {
    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT64, false)},
            new Column[]{new Column("value", NativeTypes.INT64, false)}
    );

    private static final Row FIRST_KEY = createKeyRow(0);

    private static final Row FIRST_VALUE = createKeyValueRow(0, 0);

    private static final Row SECOND_KEY = createKeyRow(1);

    private static final Row SECOND_VALUE = createKeyValueRow(1, 1);

    /**
     * Paths for created partition listeners.
     */
    private final Map<PartitionListener, Path> paths = new ConcurrentHashMap<>();

    private final List<TxManager> managers = new ArrayList<>();

    private final Function<NetworkAddress, ClusterNode> addressToNode = addr -> {
        throw new UnsupportedOperationException();
    };

    @AfterEach
    @Override
    public void afterTest() throws Exception {
        super.afterTest();

        for (TxManager txManager : managers) {
            txManager.stop();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void beforeFollowerStop(RaftGroupService service) throws Exception {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-17817 Use Replica layer with new transaction protocol.
        TxManagerImpl txManager = new TxManagerImpl(null, new HeapLockManager(), new HybridClock());

        managers.add(txManager);

        txManager.start();

        var table = new InternalTableImpl(
                "table",
                UUID.randomUUID(),
                Int2ObjectMaps.singleton(0, service),
                1,
                NetworkAddress::toString,
                addressToNode,
                txManager,
                mock(MvTableStorage.class),
                mock(TxStateTableStorage.class),
                mock(ReplicaService.class),
                mock(HybridClock.class)
        );

        table.upsert(FIRST_VALUE, null).get();
    }

    /** {@inheritDoc} */
    @Override
    public void afterFollowerStop(RaftGroupService service) throws Exception {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-17817 Use Replica layer with new transaction protocol.
        TxManagerImpl txManager = new TxManagerImpl(null, new HeapLockManager(), new HybridClock());

        managers.add(txManager);

        txManager.start();

        var table = new InternalTableImpl(
                "table",
                UUID.randomUUID(),
                Int2ObjectMaps.singleton(0, service),
                1,
                NetworkAddress::toString,
                addressToNode,
                txManager,
                mock(MvTableStorage.class),
                mock(TxStateTableStorage.class),
                mock(ReplicaService.class),
                mock(HybridClock.class)
        );

        // Remove the first key
        table.delete(FIRST_KEY, null).get();

        // Put deleted data again
        table.upsert(FIRST_VALUE, null).get();

        txManager.stop();
    }

    /** {@inheritDoc} */
    @Override
    public void afterSnapshot(RaftGroupService service) throws Exception {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-17817 Use Replica layer with new transaction protocol.
        TxManager txManager = new TxManagerImpl(null, new HeapLockManager(), new HybridClock());

        managers.add(txManager);

        txManager.start();

        var table = new InternalTableImpl(
                "table",
                UUID.randomUUID(),
                Int2ObjectMaps.singleton(0, service),
                1,
                NetworkAddress::toString,
                addressToNode,
                txManager,
                mock(MvTableStorage.class),
                mock(TxStateTableStorage.class),
                mock(ReplicaService.class),
                mock(HybridClock.class)
        );

        table.upsert(SECOND_VALUE, null).get();

        assertNotNull(table.get(SECOND_KEY, null).join());

        txManager.stop();
    }

    /** {@inheritDoc} */
    @Override
    public BooleanSupplier snapshotCheckClosure(JraftServerImpl restarted, boolean interactedAfterSnapshot) {
        MvPartitionStorage storage = getListener(restarted, raftGroupId()).getStorage();
        Map<ByteBuffer, RowId> primaryIndex = getListener(restarted, raftGroupId()).getPk();

        Row key = interactedAfterSnapshot ? SECOND_KEY : FIRST_KEY;
        Row value = interactedAfterSnapshot ? SECOND_VALUE : FIRST_VALUE;

        return () -> {
            ReadResult read = storage.read(primaryIndex.get(key.keySlice()), HybridTimestamp.MAX_VALUE);

            if (read == null) {
                return false;
            }

            return Arrays.equals(value.bytes(), read.binaryRow().bytes());
        };
    }

    /** {@inheritDoc} */
    @Override
    public Path getListenerPersistencePath(PartitionListener listener) {
        return paths.get(listener);
    }

    /** {@inheritDoc} */
    @Override
    // TODO: https://issues.apache.org/jira/browse/IGNITE-17817 Use Replica layer with new transaction protocol.
    public RaftGroupListener createListener(ClusterService service, Path workDir) {
        return paths.entrySet().stream()
                .filter(entry -> entry.getValue().equals(workDir))
                .map(Map.Entry::getKey)
                .findAny()
                .orElseGet(() -> {
                    TxManagerImpl txManager = new TxManagerImpl(null, new HeapLockManager(), new HybridClock());

                    txManager.start(); // Init listener.

                    var testMpPartStorage = new TestMvPartitionStorage(0);

                    PartitionListener listener = new PartitionListener(
                            testMpPartStorage,
                            new TestConcurrentHashMapTxStateStorage(),
                            txManager,
                            new ConcurrentHashMap<>());

                    paths.put(listener, workDir);

                    return listener;
                });
    }

    /** {@inheritDoc} */
    @Override
    public String raftGroupId() {
        return "partitions";
    }

    /**
     * Creates a {@link Row} with the supplied key.
     *
     * @param id Key.
     * @return Row.
     */
    private static Row createKeyRow(long id) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendLong(id);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }

    /**
     * Creates a {@link Row} with the supplied key and value.
     *
     * @param id    Key.
     * @param value Value.
     * @return Row.
     */
    private static Row createKeyValueRow(long id, long value) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendLong(id);
        rowBuilder.appendLong(value);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }
}
