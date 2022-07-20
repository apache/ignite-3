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

package org.apache.ignite.internal.tx.storage.state;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(WorkDirectoryExtension.class)
public class TxStateStorageTest {
    @WorkDirectory
    protected Path workDir;

    @Test
    public void testPutGetRemove() throws Exception {
        try (TxStateStorage storage = createStorage()) {
            storage.start();

            List<UUID> txIds = new ArrayList<>();

            for (int i = 0; i < 100; i++) {
                UUID txId = UUID.randomUUID();

                txIds.add(txId);

                storage.put(txId, new TxMeta(TxState.PENDING, new ArrayList<>(), new Timestamp(txId)));
            }

            for (int i = 0; i < 100; i++) {
                TxMeta txMeta = storage.get(txIds.get(i));
                TxMeta txMetaExpected = new TxMeta(TxState.PENDING, new ArrayList<>(), new Timestamp(txIds.get(i)));
                assertTxMetaEquals(txMetaExpected, txMeta);
            }

            for (int i = 0; i < 100; i++) {
                if (i % 2 == 0) {
                    storage.remove(txIds.get(i));
                }
            }

            for (int i = 0; i < 100; i++) {
                if (i % 2 == 0) {
                    TxMeta txMeta = storage.get(txIds.get(i));
                    assertNull(txMeta);
                } else {
                    TxMeta txMeta = storage.get(txIds.get(i));
                    TxMeta txMetaExpected = new TxMeta(TxState.PENDING, new ArrayList<>(), new Timestamp(txIds.get(i)));
                    assertTxMetaEquals(txMetaExpected, txMeta);
                }
            }
        }
    }

    @Test
    public void testCas() throws Exception {
        try (TxStateStorage storage = createStorage()) {
            storage.start();

            UUID txId = UUID.randomUUID();

            TxMeta txMeta0 = new TxMeta(TxState.PENDING, new ArrayList<>(), new Timestamp(txId));
            TxMeta txMeta1 = new TxMeta(TxState.COMMITED, new ArrayList<>(), new Timestamp(txId));
            TxMeta txMeta2 = new TxMeta(TxState.COMMITED, new ArrayList<>(), new Timestamp(UUID.randomUUID()));

            storage.put(txId, txMeta0);

            assertTxMetaEquals(storage.get(txId), txMeta0);

            assertFalse(storage.compareAndSet(txId, txMeta1.txState(), txMeta2));
            assertTrue(storage.compareAndSet(txId, txMeta0.txState(), txMeta2));

            assertTxMetaEquals(storage.get(txId), txMeta2);
        }
    }

    @Test
    public void testSnapshot() throws Exception {
        try (TxStateStorage storage = createStorage()) {
            storage.start();

            List<UUID> inSnapshot = new ArrayList<>();

            for (int i = 0; i < 100; i++) {
                UUID txId = UUID.randomUUID();

                storage.put(txId, new TxMeta(TxState.COMMITED, new ArrayList<>(), new Timestamp(txId)));

                inSnapshot.add(txId);
            }

            Path snapshotDirPath = workDir.resolve("snapshot");
            Files.createDirectories(snapshotDirPath);

            try {
                storage.snapshot(snapshotDirPath).join();

                List<UUID> notInSnapshot = new ArrayList<>();

                for (int i = 0; i < 100; i++) {
                    UUID txId = UUID.randomUUID();

                    storage.put(txId, new TxMeta(TxState.COMMITED, new ArrayList<>(), new Timestamp(txId)));

                    notInSnapshot.add(txId);
                }

                for (int i = 0; i < 100; i++) {
                    UUID txId = notInSnapshot.get(i);

                    assertTxMetaEquals(new TxMeta(TxState.COMMITED, new ArrayList<>(), new Timestamp(txId)), storage.get(txId));
                }

                storage.restoreSnapshot(snapshotDirPath);

                for (int i = 0; i < 100; i++) {
                    UUID txId = inSnapshot.get(i);

                    assertTxMetaEquals(new TxMeta(TxState.COMMITED, new ArrayList<>(), new Timestamp(txId)), storage.get(txId));
                }

                for (int i = 0; i < 100; i++) {
                    UUID txId = notInSnapshot.get(i);

                    assertNull(storage.get(txId));
                }
            } finally {
                Files.walk(snapshotDirPath)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    private static void assertTxMetaEquals(TxMeta txMeta0, TxMeta txMeta1) {
        assertEquals(txMeta0.txState(), txMeta1.txState());
        assertEquals(txMeta0.commitTimestamp(), txMeta1.commitTimestamp());
        assertEquals(txMeta0.enlistedPartitions(), txMeta1.enlistedPartitions());
    }

    private TxStateStorage createStorage() {
        return new TxStateRocksDbStorage(workDir, Executors.newSingleThreadExecutor());
    }
}
