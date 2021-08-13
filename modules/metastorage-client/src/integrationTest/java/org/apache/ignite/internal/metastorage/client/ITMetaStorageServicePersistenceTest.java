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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDBKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.raft.client.service.ITAbstractListenerSnapshotTest;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Persistent (rocksdb-based) meta storage raft group snapshots tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ITMetaStorageServicePersistenceTest extends ITAbstractListenerSnapshotTest<MetaStorageListener> {
    /** */
    private final ByteArray firstKey = ByteArray.fromString("first");

    /** */
    private final byte[] firstValue = "firstValue".getBytes(StandardCharsets.UTF_8);

    /** */
    private final ByteArray secondKey = ByteArray.fromString("second");

    /** */
    private final byte[] secondValue = "secondValue".getBytes(StandardCharsets.UTF_8);

    /** */
    private MetaStorageServiceImpl metaStorage;

    /** {@inheritDoc} */
    @Override public void doBeforeStop(RaftGroupService service) throws Exception {
        metaStorage = new MetaStorageServiceImpl(service, null);

        // Put some data in the metastorage
        metaStorage.put(firstKey, firstValue).get();

        // Check that data has been written successfully
        check(metaStorage, new EntryImpl(firstKey, firstValue, 1, 1));;
    }

    /** {@inheritDoc} */
    @Override public void doAfterStop(RaftGroupService service) throws Exception {
        // Remove the first key from the metastorage
        metaStorage.remove(firstKey).get();

        // Check that data has been removed
        check(metaStorage, new EntryImpl(firstKey, null, 2, 2));

        // Put same data again
        metaStorage.put(firstKey, firstValue).get();

        // Check that it has been written
        check(metaStorage, new EntryImpl(firstKey, firstValue, 3, 3));
    }

    /** {@inheritDoc} */
    @Override public void doAfterSnapshot(RaftGroupService service) throws Exception {
        metaStorage.put(secondKey, secondValue).get();
    }

    /** {@inheritDoc} */
    @Override public BooleanSupplier snapshotCheckClosure(JRaftServerImpl restarted, boolean interactedAfterSnapshot) {
        KeyValueStorage storage = getListener(restarted, raftGroupId()).getStorage();

        byte[] lastKey = interactedAfterSnapshot ? secondKey.bytes() : firstKey.bytes();
        byte[] lastValue = interactedAfterSnapshot ? secondValue : firstValue;

        int expectedRevision = interactedAfterSnapshot ? 4 : 3;
        int expectedUpdateCounter = interactedAfterSnapshot ? 4 : 3;

        EntryImpl expectedLastEntry = new EntryImpl(new ByteArray(lastKey), lastValue, expectedRevision, expectedUpdateCounter);

        return () -> {
            org.apache.ignite.internal.metastorage.server.Entry e = storage.get(lastKey);
            return e.empty() == expectedLastEntry.empty()
                && e.tombstone() == expectedLastEntry.tombstone()
                && e.revision() == expectedLastEntry.revision()
                && e.updateCounter() == expectedLastEntry.revision()
                && Arrays.equals(e.key(), expectedLastEntry.key().bytes())
                && Arrays.equals(e.value(), expectedLastEntry.value());
        };
    }

    /** {@inheritDoc} */
    @Override public Path getListenerPersistencePath(MetaStorageListener listener) {
        return ((RocksDBKeyValueStorage) listener.getStorage()).getDbPath();
    }

    /** {@inheritDoc} */
    @Override public RaftGroupListener createListener(Path workDir) {
        return new MetaStorageListener(new RocksDBKeyValueStorage(workDir.resolve(UUID.randomUUID().toString())));
    }

    /** {@inheritDoc} */
    @Override public String raftGroupId() {
        return "metastorage";
    }

    /**
     * Check meta storage entry.
     *
     * @param metaStorage Meta storage service.
     * @param expected Expected entry.
     * @throws ExecutionException If failed.
     * @throws InterruptedException If failed.
     */
    private void check(MetaStorageServiceImpl metaStorage, EntryImpl expected)
        throws ExecutionException, InterruptedException {
        Entry entry = metaStorage.get(expected.key()).get();

        assertEquals(expected, entry);
    }
}
