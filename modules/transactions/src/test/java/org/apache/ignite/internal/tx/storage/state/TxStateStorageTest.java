/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.tx.storage.state;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(WorkDirectoryExtension.class)
public class TxStateStorageTest {
    @WorkDirectory
    protected Path workDir;

    @Test
    public void test() throws Exception {
        try (TxStateStorage storage = createStorage()) {
            storage.start();

            TxMeta txMeta0 = new TxMeta(TxState.PENDING, new ArrayList<>(), 0L);
            TxMeta txMeta1 = new TxMeta(TxState.COMMITED, new ArrayList<>(), 0L);
            TxMeta txMeta2 = new TxMeta(TxState.COMMITED, new ArrayList<>(), 1L);

            UUID txId = UUID.randomUUID();

            storage.put(txId, txMeta0);

            assertTxMetaEquals(storage.get(txId), txMeta0);

            assertFalse(storage.compareAndSet(txId, txMeta1.txState(), txMeta2));
            assertTrue(storage.compareAndSet(txId, txMeta0.txState(), txMeta2));

            assertTxMetaEquals(storage.get(txId), txMeta2);
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
