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

package org.apache.ignite.internal.compute;

import static java.lang.Thread.sleep;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.wrapper.Wrappers.unwrapNullable;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
import org.apache.ignite.internal.pagememory.persistence.compaction.Compactor;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.wrapper.Wrappers;

/** A job that forces a checkpoint and optionally cancels the compaction process on the node. */
public class CheckpointJob implements ComputeJob<Boolean, Void> {
    @Override
    public CompletableFuture<Void> executeAsync(JobExecutionContext context, Boolean shouldCancelCompaction) {

        try {
            IgniteImpl igniteImpl = Wrappers.unwrap(context.ignite(), IgniteImpl.class);

            CheckpointManager checkpointManager = checkpointManager(igniteImpl);

            if (shouldCancelCompaction) {
                Field compactorField = checkpointManager.getClass().getDeclaredField("compactor");
                compactorField.setAccessible(true);

                Compactor compactor = (Compactor) compactorField.get(checkpointManager);

                compactor.cancel();
            }

            return checkpointManager.forceCheckpoint("test").futureFor(CheckpointState.FINISHED).thenRun(() -> {
                if (!shouldCancelCompaction) {
                    try {
                        waitForCompaction(checkpointManager);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    private static void waitForCompaction(CheckpointManager checkpointManager) throws Exception {
        Field filePageStoreManagerField = checkpointManager.getClass().getDeclaredField("filePageStoreManager");
        filePageStoreManagerField.setAccessible(true);

        FilePageStoreManager pageStoreManager = (FilePageStoreManager) filePageStoreManagerField.get(checkpointManager);

        boolean compacted = waitForCondition(
                () -> pageStoreManager.allPageStores()
                        .allMatch(pageStore -> pageStore.pageStore().deltaFileCount() == 0),
                10_000
        );

        assert compacted;
    }

    private static CheckpointManager checkpointManager(IgniteImpl igniteImpl) throws Exception {
        Field dataStorageMgrField = igniteImpl.getClass().getDeclaredField("dataStorageMgr");
        dataStorageMgrField.setAccessible(true);

        DataStorageManager dataStorageManager = (DataStorageManager) dataStorageMgrField.get(igniteImpl);

        PersistentPageMemoryStorageEngine engine = unwrapNullable(dataStorageManager
                .engineByStorageProfile("default_aipersist"), PersistentPageMemoryStorageEngine.class);

        if (engine == null) {
            throw new IllegalStateException("PersistentPageMemoryStorageEngine not found");
        }

        CheckpointManager checkpointManager = engine.checkpointManager();

        if (checkpointManager == null) {
            throw new IllegalStateException("CheckpointManager is null");
        }

        return checkpointManager;
    }

    @SuppressWarnings("BusyWait")
    private static boolean waitForCondition(BooleanSupplier cond, long timeoutMillis) throws InterruptedException {
        long stop = System.currentTimeMillis() + timeoutMillis;

        while (System.currentTimeMillis() < stop) {
            if (cond.getAsBoolean()) {
                return true;
            }
            sleep(10);
        }

        return false;
    }
}
