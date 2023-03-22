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

package org.apache.ignite.internal.table.distributed.gc;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willFailFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willTimeoutFast;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.lang.ErrorGroups.GarbageCollector;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

/**
 * For testing {@link MvGc}.
 */
@ExtendWith(ConfigurationExtension.class)
public class MvGcTest {
    private static final int PARTITION_ID = 0;

    private MvGc gc;

    @BeforeEach
    void setUp(
            @InjectConfiguration("mock.gcThreads = 1")
            TablesConfiguration tablesConfig
    ) {
        gc = new MvGc("test", tablesConfig);

        gc.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(gc);
    }

    @Test
    void testAddStorageWithoutLowWatermark() {
        CompletableFuture<Void> invokeVacuumMethodFuture = new CompletableFuture<>();

        gc.addStorage(createTablePartitionId(), createWithCompleteFutureOnVacuum(invokeVacuumMethodFuture, null));

        // We expect that StorageUpdateHandler#vacuum will not be called.
        assertThat(invokeVacuumMethodFuture, willTimeoutFast());
    }

    @Test
    void testAddStorageWithLowWatermark() {
        HybridTimestamp lowWatermark = new HybridTimestamp(1, 1);

        gc.updateLowWatermark(lowWatermark);

        CompletableFuture<Void> invokeVacuumMethodFuture = new CompletableFuture<>();

        gc.addStorage(createTablePartitionId(), createWithCompleteFutureOnVacuum(invokeVacuumMethodFuture, lowWatermark));

        // We expect StorageUpdateHandler#vacuum to be called with the set low watermark.
        assertThat(invokeVacuumMethodFuture, willCompleteSuccessfully());
    }

    @Test
    void testStartVacuumOnSuccessfulUpdateLowWatermark() {
        CompletableFuture<Void> invokeVacuumMethodFuture0 = new CompletableFuture<>();
        CompletableFuture<Void> invokeVacuumMethodFuture1 = new CompletableFuture<>();

        HybridTimestamp lowWatermark0 = new HybridTimestamp(1, 1);

        StorageUpdateHandler storageUpdateHandler0 = createWithCompleteFutureOnVacuum(invokeVacuumMethodFuture0, lowWatermark0);
        StorageUpdateHandler storageUpdateHandler1 = createWithCompleteFutureOnVacuum(invokeVacuumMethodFuture1, lowWatermark0);

        gc.addStorage(createTablePartitionId(), storageUpdateHandler0);
        gc.addStorage(createTablePartitionId(), storageUpdateHandler1);

        gc.updateLowWatermark(lowWatermark0);

        // We expect StorageUpdateHandler#vacuum to be called with the set lowWatermark0.
        assertThat(invokeVacuumMethodFuture0, willCompleteSuccessfully());
        assertThat(invokeVacuumMethodFuture1, willCompleteSuccessfully());

        // What happens if we increase low watermark ?
        CompletableFuture<Void> invokeVacuumMethodFuture2 = new CompletableFuture<>();
        CompletableFuture<Void> invokeVacuumMethodFuture3 = new CompletableFuture<>();

        HybridTimestamp lowWatermark1 = new HybridTimestamp(2, 2);

        completeFutureOnVacuum(storageUpdateHandler0, invokeVacuumMethodFuture2, lowWatermark1);
        completeFutureOnVacuum(storageUpdateHandler1, invokeVacuumMethodFuture3, lowWatermark1);

        gc.updateLowWatermark(lowWatermark1);

        // We expect StorageUpdateHandler#vacuum to be called with the set lowWatermark0.
        assertThat(invokeVacuumMethodFuture2, willCompleteSuccessfully());
        assertThat(invokeVacuumMethodFuture3, willCompleteSuccessfully());
    }

    @Test
    void testStartVacuumOnFailUpdateLowWatermark() {
        HybridTimestamp firstLowWatermark = new HybridTimestamp(2, 2);

        CompletableFuture<Void> invokeVacuumMethodFuture0 = new CompletableFuture<>();
        CompletableFuture<Void> invokeVacuumMethodFuture1 = new CompletableFuture<>();

        StorageUpdateHandler storageUpdateHandler0 = createWithCompleteFutureOnVacuum(invokeVacuumMethodFuture0, firstLowWatermark);
        StorageUpdateHandler storageUpdateHandler1 = createWithCompleteFutureOnVacuum(invokeVacuumMethodFuture1, firstLowWatermark);

        gc.addStorage(createTablePartitionId(), storageUpdateHandler0);
        gc.addStorage(createTablePartitionId(), storageUpdateHandler1);

        gc.updateLowWatermark(firstLowWatermark);

        // We expect StorageUpdateHandler#vacuum to be called with the set lowWatermark0.
        assertThat(invokeVacuumMethodFuture0, willCompleteSuccessfully());
        assertThat(invokeVacuumMethodFuture1, willCompleteSuccessfully());

        // What happens if we try set same low watermark ?
        HybridTimestamp sameLowWatermark = new HybridTimestamp(2, 2);

        CompletableFuture<Void> invokeVacuumMethodFutureForSame0 = new CompletableFuture<>();
        CompletableFuture<Void> invokeVacuumMethodFutureForSame1 = new CompletableFuture<>();

        completeFutureOnVacuum(storageUpdateHandler0, invokeVacuumMethodFutureForSame0, sameLowWatermark);
        completeFutureOnVacuum(storageUpdateHandler1, invokeVacuumMethodFutureForSame1, sameLowWatermark);

        gc.updateLowWatermark(sameLowWatermark);

        // We expect that StorageUpdateHandler#vacuum will not be called.
        assertThat(invokeVacuumMethodFutureForSame0, willTimeoutFast());
        assertThat(invokeVacuumMethodFutureForSame1, willTimeoutFast());

        // What happens if we try set same lower watermark ?
        HybridTimestamp lowerLowWatermark = new HybridTimestamp(1, 1);

        CompletableFuture<Void> invokeVacuumMethodFutureForLower0 = new CompletableFuture<>();
        CompletableFuture<Void> invokeVacuumMethodFutureForLower1 = new CompletableFuture<>();

        completeFutureOnVacuum(storageUpdateHandler0, invokeVacuumMethodFutureForLower0, lowerLowWatermark);
        completeFutureOnVacuum(storageUpdateHandler1, invokeVacuumMethodFutureForLower1, lowerLowWatermark);

        gc.updateLowWatermark(lowerLowWatermark);

        // We expect that StorageUpdateHandler#vacuum will not be called.
        assertThat(invokeVacuumMethodFutureForSame0, willTimeoutFast());
        assertThat(invokeVacuumMethodFutureForSame1, willTimeoutFast());
        assertThat(invokeVacuumMethodFutureForLower0, willTimeoutFast());
        assertThat(invokeVacuumMethodFutureForLower1, willTimeoutFast());
    }

    @Test
    void testCountInvokeVacuum() throws Exception {
        CountDownLatch latch = new CountDownLatch(MvGc.GC_BATCH_SIZE + 2);

        StorageUpdateHandler storageUpdateHandler = createWithCountDownOnVacuum(latch);

        gc.addStorage(createTablePartitionId(), storageUpdateHandler);

        gc.updateLowWatermark(new HybridTimestamp(2, 2));

        assertTrue(latch.await(200, TimeUnit.MILLISECONDS));
    }

    @Test
    void testRemoveStorageNotExist() {
        assertThat(gc.removeStorage(createTablePartitionId()), willCompleteSuccessfully());
    }

    @Test
    void testRemoveStorageForCompletedGc() {
        CompletableFuture<Void> invokeVacuumMethodFuture = new CompletableFuture<>();

        TablePartitionId tablePartitionId = createTablePartitionId();

        gc.addStorage(tablePartitionId, createWithCompleteFutureOnVacuum(invokeVacuumMethodFuture, null));

        gc.updateLowWatermark(new HybridTimestamp(1, 1));

        assertThat(invokeVacuumMethodFuture, willCompleteSuccessfully());
        assertThat(gc.removeStorage(tablePartitionId), willCompleteSuccessfully());

        // What happens if we delete it again?
        assertThat(gc.removeStorage(tablePartitionId), willCompleteSuccessfully());
    }

    @Test
    void testRemoveStorageInMiddleGc() {
        CompletableFuture<Void> startInvokeVacuumMethodFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishInvokeVacuumMethodFuture = new CompletableFuture<>();

        TablePartitionId tablePartitionId = createTablePartitionId();

        gc.addStorage(tablePartitionId, createWithWaitFinishVacuum(startInvokeVacuumMethodFuture, finishInvokeVacuumMethodFuture));

        gc.updateLowWatermark(new HybridTimestamp(1, 1));

        assertThat(startInvokeVacuumMethodFuture, willCompleteSuccessfully());

        CompletableFuture<Void> removeStorageFuture = gc.removeStorage(tablePartitionId);

        assertThat(removeStorageFuture, willTimeoutFast());

        finishInvokeVacuumMethodFuture.complete(null);

        assertThat(removeStorageFuture, willCompleteSuccessfully());
    }

    @Test
    void testRemoveStorageWithError() {
        CompletableFuture<Void> startInvokeVacuumMethodFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishInvokeVacuumMethodFuture = new CompletableFuture<>();

        TablePartitionId tablePartitionId = createTablePartitionId();

        gc.addStorage(tablePartitionId, createWithWaitFinishVacuum(startInvokeVacuumMethodFuture, finishInvokeVacuumMethodFuture));

        gc.updateLowWatermark(new HybridTimestamp(1, 1));

        assertThat(startInvokeVacuumMethodFuture, willCompleteSuccessfully());

        CompletableFuture<Void> removeStorageFuture = gc.removeStorage(tablePartitionId);

        assertThat(removeStorageFuture, willTimeoutFast());

        finishInvokeVacuumMethodFuture.completeExceptionally(new RuntimeException("form test"));

        assertThat(removeStorageFuture, willFailFast(RuntimeException.class));
    }

    @Test
    void testRemoveStorage() {
        CompletableFuture<Void> invokeVacuumMethodFuture0 = new CompletableFuture<>();

        TablePartitionId tablePartitionId = createTablePartitionId();

        StorageUpdateHandler storageUpdateHandler = createWithCompleteFutureOnVacuum(invokeVacuumMethodFuture0, null);

        gc.addStorage(tablePartitionId, storageUpdateHandler);

        gc.updateLowWatermark(new HybridTimestamp(1, 1));

        assertThat(invokeVacuumMethodFuture0, willCompleteSuccessfully());
        assertThat(gc.removeStorage(tablePartitionId), willCompleteSuccessfully());

        // What happens if we update the low watermark?
        CompletableFuture<Void> invokeVacuumMethodFuture1 = new CompletableFuture<>();

        completeFutureOnVacuum(storageUpdateHandler, invokeVacuumMethodFuture1, null);

        assertThat(invokeVacuumMethodFuture1, willTimeoutFast());
    }

    @Test
    void testClose() throws Exception {
        gc.close();

        assertThrowsClosed(() -> gc.addStorage(createTablePartitionId(), mock(StorageUpdateHandler.class)));
        assertThrowsClosed(() -> gc.removeStorage(createTablePartitionId()));
        assertThrowsClosed(() -> gc.updateLowWatermark(new HybridTimestamp(1, 1)));

        assertDoesNotThrow(gc::close);
    }

    @Test
    void testParallelUpdateLowWatermark(
            @InjectConfiguration
            TablesConfiguration tablesConfig
    ) throws Exception {
        // By default, in the tests we work in one thread, we don’t have enough this, we will add more.
        assertThat(tablesConfig.gcThreads().update(Runtime.getRuntime().availableProcessors()), willCompleteSuccessfully());

        gc.close();

        gc = new MvGc("test", tablesConfig);

        gc.start();

        gc.updateLowWatermark(new HybridTimestamp(1, 1));

        for (int i = 0; i < 100; i++) {
            CountDownLatch latch = new CountDownLatch(5);

            TablePartitionId tablePartitionId = createTablePartitionId();

            gc.addStorage(tablePartitionId, createWithCountDownOnVacuumWithoutNextBatch(latch));

            runRace(
                    () -> gc.scheduleGcForAllStorages(),
                    () -> gc.scheduleGcForAllStorages(),
                    () -> gc.scheduleGcForAllStorages(),
                    () -> gc.scheduleGcForAllStorages()
            );

            // We will check that we will call the vacuum on each update of the low watermark.
            assertTrue(latch.await(200, TimeUnit.MILLISECONDS), "remaining=" + latch.getCount());

            assertThat(gc.removeStorage(tablePartitionId), willCompleteSuccessfully());
        }
    }

    private TablePartitionId createTablePartitionId() {
        return new TablePartitionId(UUID.randomUUID(), PARTITION_ID);
    }

    private StorageUpdateHandler createWithCompleteFutureOnVacuum(CompletableFuture<Void> future, @Nullable HybridTimestamp exp) {
        StorageUpdateHandler storageUpdateHandler = mock(StorageUpdateHandler.class);

        completeFutureOnVacuum(storageUpdateHandler, future, exp);

        return storageUpdateHandler;
    }

    private void completeFutureOnVacuum(
            StorageUpdateHandler storageUpdateHandler,
            CompletableFuture<Void> future,
            @Nullable HybridTimestamp exp
    ) {
        when(storageUpdateHandler.vacuum(any(HybridTimestamp.class))).then(invocation -> {
            if (exp != null) {
                try {
                    assertEquals(exp, invocation.getArgument(0));

                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            } else {
                future.complete(null);
            }

            return false;
        });
    }

    private StorageUpdateHandler createWithCountDownOnVacuum(CountDownLatch latch) {
        StorageUpdateHandler storageUpdateHandler = mock(StorageUpdateHandler.class);

        when(storageUpdateHandler.vacuum(any(HybridTimestamp.class))).then(invocation -> {
            latch.countDown();

            return latch.getCount() > 0;
        });

        return storageUpdateHandler;
    }

    private StorageUpdateHandler createWithWaitFinishVacuum(CompletableFuture<Void> startFuture, CompletableFuture<Void> finishFuture) {
        StorageUpdateHandler storageUpdateHandler = mock(StorageUpdateHandler.class);

        when(storageUpdateHandler.vacuum(any(HybridTimestamp.class))).then(invocation -> {
            startFuture.complete(null);

            assertThat(finishFuture, willCompleteSuccessfully());

            return false;
        });

        return storageUpdateHandler;
    }

    private static void assertThrowsClosed(Executable executable) {
        IgniteInternalException exception = assertThrows(IgniteInternalException.class, executable);

        assertEquals(GarbageCollector.CLOSED_ERR, exception.code());
    }

    private StorageUpdateHandler createWithCountDownOnVacuumWithoutNextBatch(CountDownLatch latch) {
        StorageUpdateHandler storageUpdateHandler = mock(StorageUpdateHandler.class);

        when(storageUpdateHandler.vacuum(any(HybridTimestamp.class))).then(invocation -> {
            latch.countDown();

            // So that there is no processing of the next batch.
            return false;
        });

        return storageUpdateHandler;
    }
}
