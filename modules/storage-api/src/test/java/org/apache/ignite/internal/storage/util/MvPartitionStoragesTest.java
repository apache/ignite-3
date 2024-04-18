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

package org.apache.ignite.internal.storage.util;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class for testing {@link MvPartitionStorages}.
 */
public class MvPartitionStoragesTest extends BaseIgniteAbstractTest {
    private static final int PARTITIONS = 10;

    private MvPartitionStorages<MvPartitionStorage> mvPartitionStorages;

    @BeforeEach
    void setUp() {
        mvPartitionStorages = new MvPartitionStorages(0, PARTITIONS);
    }

    @Test
    void testGet() {
        assertThrows(IllegalArgumentException.class, () -> getMvStorage(getPartitionIdOutOfConfig()));

        assertNull(getMvStorage(0));

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        assertNotNull(getMvStorage(0));

        assertNull(getMvStorage(1));
    }

    @Test
    void testCreate() {
        assertThat(createMvStorage(0), willCompleteSuccessfully());

        CompletableFuture<Void> startCreateMvStorageFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishCreateMvStorageFuture = new CompletableFuture<>();

        CompletableFuture<?> createMvStorageFuture = runAsync(() ->
                assertThat(mvPartitionStorages.create(1, partId -> {
                    startCreateMvStorageFuture.complete(null);

                    assertThat(finishCreateMvStorageFuture, willCompleteSuccessfully());

                    return mock(MvPartitionStorage.class);
                }), willCompleteSuccessfully())
        );

        assertThat(startCreateMvStorageFuture, willCompleteSuccessfully());

        assertThrowsWithCause(() -> createMvStorage(1), StorageException.class, "Storage is in process of being created");

        finishCreateMvStorageFuture.complete(null);

        assertThat(createMvStorageFuture, willCompleteSuccessfully());
    }

    @Test
    void testCreateError() {
        assertThrows(IllegalArgumentException.class, () -> createMvStorage(getPartitionIdOutOfConfig()));

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        assertThrowsWithCause(() -> createMvStorage(0), StorageException.class, "Storage already exists");

        // What if there is an error during the operation?

        assertThat(mvPartitionStorages.create(2, partId -> {
            throw new RuntimeException("from test");
        }), willThrowFast(RuntimeException.class));

        assertNull(getMvStorage(2));
    }

    @Test
    void testCreateDuringDestroy() {
        assertThat(createMvStorage(0), willCompleteSuccessfully());

        MvPartitionStorage mvStorage = getMvStorage(0);

        CompletableFuture<Void> startDestroyMvStorageFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishDestroyMvStorageFuture = new CompletableFuture<>();

        CompletableFuture<?> destroyMvStorageFuture = runAsync(() ->
                assertThat(mvPartitionStorages.destroy(0, mvStorage1 -> {
                    startDestroyMvStorageFuture.complete(null);

                    return finishDestroyMvStorageFuture;
                }), willCompleteSuccessfully())
        );

        assertThat(startDestroyMvStorageFuture, willCompleteSuccessfully());

        CompletableFuture<MvPartitionStorage> reCreateMvStorageFuture = createMvStorage(0);

        assertThrowsWithCause(() -> createMvStorage(0),
                StorageException.class,
                "Creation of the storage after its destruction is already planned"
        );

        finishDestroyMvStorageFuture.complete(null);

        assertThat(destroyMvStorageFuture, willCompleteSuccessfully());

        assertThat(reCreateMvStorageFuture, willCompleteSuccessfully());

        assertNotSame(mvStorage, getMvStorage(0));
    }

    @Test
    void testCreateDuringDestroyError() {
        assertThat(createMvStorage(0), willCompleteSuccessfully());

        // What if there is an error during the destroy?

        CompletableFuture<Void> startErrorDestroyMvStorageFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishErrorDestroyMvStorageFuture = new CompletableFuture<>();

        CompletableFuture<?> errorDestroyMvStorageFuture = mvPartitionStorages.destroy(0, mvStorage1 -> {
            startErrorDestroyMvStorageFuture.complete(null);

            return finishErrorDestroyMvStorageFuture;
        });

        assertThat(startErrorDestroyMvStorageFuture, willCompleteSuccessfully());

        CompletableFuture<MvPartitionStorage> errorReCreateMvStorageFuture = createMvStorage(0);

        finishErrorDestroyMvStorageFuture.completeExceptionally(new IllegalStateException("from test"));

        assertThat(errorDestroyMvStorageFuture, willThrowFast(IllegalStateException.class));
        assertThat(errorReCreateMvStorageFuture, willThrowFast(IllegalStateException.class));

        assertNull(getMvStorage(0));
    }

    @Test
    void testCreateAfterDestroy() {
        assertThat(createMvStorage(0), willCompleteSuccessfully());

        MvPartitionStorage mvStorage = getMvStorage(0);

        assertThat(destroyMvStorage(0), willCompleteSuccessfully());

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        assertNotSame(mvStorage, getMvStorage(0));
    }

    @Test
    void testDestroy() {
        assertThat(createMvStorage(0), willCompleteSuccessfully());

        CompletableFuture<Void> startDestroyMvStorageFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishDestroyMvStorageFuture = new CompletableFuture<>();

        CompletableFuture<?> destroyMvStorageFuture = runAsync(() ->
                assertThat(mvPartitionStorages.destroy(0, mvStorage -> {
                    startDestroyMvStorageFuture.complete(null);

                    return finishDestroyMvStorageFuture;
                }), willCompleteSuccessfully())
        );

        assertThat(startDestroyMvStorageFuture, willCompleteSuccessfully());

        assertThrowsWithCause(() -> destroyMvStorage(0), StorageException.class, "Storage does not exist");
        assertThrowsWithCause(() -> clearMvStorage(0), StorageException.class, "Storage does not exist");

        assertThrowsWithCause(() -> startRebalanceMvStorage(0), StorageRebalanceException.class, "Storage does not exist");
        assertThrowsWithCause(() -> abortRebalanceMvStorage(0), StorageRebalanceException.class, "Storage does not exist");
        assertThrowsWithCause(() -> finishRebalanceMvStorage(0), StorageRebalanceException.class, "Storage does not exist");

        finishDestroyMvStorageFuture.complete(null);

        assertThat(destroyMvStorageFuture, willCompleteSuccessfully());

        assertNull(getMvStorage(0));

        assertThat(createMvStorage(0), willCompleteSuccessfully());
    }

    @Test
    void testDestroyError() {
        assertThrows(IllegalArgumentException.class, () -> destroyMvStorage(getPartitionIdOutOfConfig()));

        assertThrowsWithCause(() -> destroyMvStorage(0), StorageException.class, "Storage does not exist");

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        // What if there is an error during the operation?

        assertThat(
                mvPartitionStorages.destroy(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willThrowFast(RuntimeException.class)
        );

        assertNull(getMvStorage(0));

        assertThrowsWithCause(() -> destroyMvStorage(0), StorageException.class, "Storage does not exist");
    }

    @Test
    void testClear() {
        assertThat(createMvStorage(0), willCompleteSuccessfully());

        MvPartitionStorage mvStorage = getMvStorage(0);

        CompletableFuture<Void> startCleanupMvStorageFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishCleanupMvStorageFuture = new CompletableFuture<>();

        CompletableFuture<?> cleanupMvStorageFuture = runAsync(() ->
                assertThat(mvPartitionStorages.clear(0, mvStorage1 -> {
                    startCleanupMvStorageFuture.complete(null);

                    return finishCleanupMvStorageFuture;
                }), willCompleteSuccessfully())
        );

        assertThat(startCleanupMvStorageFuture, willCompleteSuccessfully());

        assertThrowsWithCause(() -> destroyMvStorage(0), StorageException.class, "Storage is in process of being cleaned up");
        assertThrowsWithCause(() -> clearMvStorage(0), StorageException.class, "Storage is in process of being cleaned up");

        assertThrowsWithCause(() -> startRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage is in process of being cleaned up");
        assertThrowsWithCause(() -> abortRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage is in process of being cleaned up");
        assertThrowsWithCause(() -> finishRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage is in process of being cleaned up");

        finishCleanupMvStorageFuture.complete(null);

        assertThat(cleanupMvStorageFuture, willCompleteSuccessfully());

        assertSame(mvStorage, getMvStorage(0));

        assertThat(clearMvStorage(0), willCompleteSuccessfully());
    }

    @Test
    void testClearError() {
        assertThrows(IllegalArgumentException.class, () -> clearMvStorage(getPartitionIdOutOfConfig()));

        assertThrowsWithCause(() -> clearMvStorage(0), StorageException.class, "Storage does not exist");

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        // What if there is an error during the operation?

        assertThat(
                mvPartitionStorages.clear(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willThrowFast(RuntimeException.class)
        );

        assertNotNull(getMvStorage(0));
    }

    @Test
    void testStartRebalance() {
        assertThat(createMvStorage(0), willCompleteSuccessfully());

        CompletableFuture<Void> startStartRebalanceMvStorage = new CompletableFuture<>();
        CompletableFuture<Void> finishStartRebalanceMvStorage = new CompletableFuture<>();

        CompletableFuture<?> startRebalanceFuture = runAsync(() ->
                assertThat(mvPartitionStorages.startRebalance(0, mvStorage -> {
                    startStartRebalanceMvStorage.complete(null);

                    return finishStartRebalanceMvStorage;
                }), willCompleteSuccessfully())
        );

        assertThat(startStartRebalanceMvStorage, willCompleteSuccessfully());

        assertThrowsWithCause(() -> startRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage in the process of starting a rebalance");
        assertThrowsWithCause(() -> finishRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage in the process of starting a rebalance");

        assertThrowsWithCause(() -> destroyMvStorage(0), StorageException.class, "Storage in the process of starting a rebalance");
        assertThrowsWithCause(() -> clearMvStorage(0), StorageException.class, "Storage in the process of starting a rebalance");

        finishStartRebalanceMvStorage.complete(null);

        assertThat(startRebalanceFuture, willCompleteSuccessfully());

        assertNotNull(getMvStorage(0));

        // What if we restart the rebalance?

        assertThat(finishRebalanceMvStorage(0), willCompleteSuccessfully());
        assertThat(startRebalanceMvStorage(0), willCompleteSuccessfully());
    }

    @Test
    void testStartRebalanceError() {
        assertThrows(IllegalArgumentException.class, () -> startRebalanceMvStorage(getPartitionIdOutOfConfig()));

        assertThrowsWithCause(() -> startRebalanceMvStorage(0), StorageRebalanceException.class, "Storage does not exist");

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        // What if there is an error during the operation?

        assertThat(
                mvPartitionStorages.startRebalance(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willThrowFast(RuntimeException.class)
        );

        assertNotNull(getMvStorage(0));

        // What if we restart the rebalance?

        assertThat(abortRebalanceMvStorage(0), willCompleteSuccessfully());

        assertThat(startRebalanceMvStorage(0), willCompleteSuccessfully());

        assertThrowsWithCause(
                () -> startRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage in the process of rebalance");
    }

    @Test
    void testAbortRebalance() {
        assertThat(createMvStorage(0), willCompleteSuccessfully());

        assertThat(startRebalanceMvStorage(0), willCompleteSuccessfully());

        CompletableFuture<Void> startAbortRebalanceFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishAbortRebalanceFuture = new CompletableFuture<>();

        CompletableFuture<?> abortRebalanceFuture = runAsync(() ->
                assertThat(mvPartitionStorages.abortRebalance(0, mvStorage -> {
                    startAbortRebalanceFuture.complete(null);

                    return finishAbortRebalanceFuture;
                }), willCompleteSuccessfully())
        );

        assertThat(startAbortRebalanceFuture, willCompleteSuccessfully());

        assertThrowsWithCause(() -> startRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage in the process of aborting a rebalance");
        assertThrowsWithCause(() -> abortRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage in the process of aborting a rebalance");
        assertThrowsWithCause(() -> finishRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage in the process of aborting a rebalance");

        assertThrowsWithCause(() -> destroyMvStorage(0), StorageException.class, "Storage in the process of aborting a rebalance");
        assertThrowsWithCause(() -> clearMvStorage(0), StorageException.class, "Storage in the process of aborting a rebalance");

        finishAbortRebalanceFuture.complete(null);

        assertThat(abortRebalanceFuture, willCompleteSuccessfully());

        assertNotNull(getMvStorage(0));

        // What if the rebalancing didn't start?

        AtomicBoolean invokeAbortFunction = new AtomicBoolean();

        assertThat(mvPartitionStorages.abortRebalance(0, mvStorage -> {
            invokeAbortFunction.set(true);

            return nullCompletedFuture();
        }), willCompleteSuccessfully());

        assertFalse(invokeAbortFunction.get());

        // What if the start of the rebalance ended with an error?

        invokeAbortFunction.set(false);

        assertThat(
                mvPartitionStorages.startRebalance(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willThrowFast(RuntimeException.class)
        );

        assertThat(mvPartitionStorages.abortRebalance(0, mvStorage -> {
            invokeAbortFunction.set(true);

            return nullCompletedFuture();
        }), willCompleteSuccessfully());

        assertTrue(invokeAbortFunction.get());
    }

    @Test
    void testAbortRebalanceError() {
        assertThrows(IllegalArgumentException.class, () -> abortRebalanceMvStorage(getPartitionIdOutOfConfig()));

        assertThrowsWithCause(() -> abortRebalanceMvStorage(0), StorageRebalanceException.class, "Storage does not exist");

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        assertThat(startRebalanceMvStorage(0), willCompleteSuccessfully());

        // What if there is an error during the operation?

        assertThat(
                mvPartitionStorages.abortRebalance(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willThrowFast(RuntimeException.class)
        );
    }

    @Test
    void testFinishRebalance() {
        assertThat(createMvStorage(0), willCompleteSuccessfully());

        assertThat(startRebalanceMvStorage(0), willCompleteSuccessfully());

        CompletableFuture<Void> startFinishRebalanceFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishFinishRebalanceFuture = new CompletableFuture<>();

        CompletableFuture<?> finishRebalanceFuture = runAsync(() ->
                assertThat(mvPartitionStorages.finishRebalance(0, mvStorage -> {
                    startFinishRebalanceFuture.complete(null);

                    return finishFinishRebalanceFuture;
                }), willCompleteSuccessfully())
        );

        assertThat(startFinishRebalanceFuture, willCompleteSuccessfully());

        assertThrowsWithCause(() -> startRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage in the process of finishing a rebalance");
        assertThrowsWithCause(() -> abortRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage in the process of finishing a rebalance");
        assertThrowsWithCause(() -> finishRebalanceMvStorage(0),
                StorageRebalanceException.class,
                "Storage in the process of finishing a rebalance");

        assertThrowsWithCause(() -> destroyMvStorage(0), StorageException.class, "Storage in the process of finishing a rebalance");
        assertThrowsWithCause(() -> clearMvStorage(0), StorageException.class, "Storage in the process of finishing a rebalance");

        finishFinishRebalanceFuture.complete(null);

        assertThat(finishRebalanceFuture, willCompleteSuccessfully());

        assertNotNull(getMvStorage(0));
    }

    @Test
    void testFinishRebalanceError() {
        assertThrows(IllegalArgumentException.class, () -> finishRebalanceMvStorage(getPartitionIdOutOfConfig()));

        assertThrowsWithCause(() -> finishRebalanceMvStorage(0), StorageRebalanceException.class, "Storage does not exist");

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        assertThrowsWithCause(() -> finishRebalanceMvStorage(0), StorageRebalanceException.class, "Storage rebalancing did not start");

        assertThat(startRebalanceMvStorage(0), willCompleteSuccessfully());

        // What if there is an error during the operation?

        assertThat(
                mvPartitionStorages.finishRebalance(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willThrowFast(RuntimeException.class)
        );

        // What if the start of the rebalance fails?

        assertThat(abortRebalanceMvStorage(0), willCompleteSuccessfully());

        assertThat(
                mvPartitionStorages.startRebalance(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willThrowFast(RuntimeException.class)
        );

        assertThat(finishRebalanceMvStorage(0), willThrowFast(RuntimeException.class));
    }

    @Test
    void testGetAllForCloseOrDestroy() {
        CompletableFuture<MvPartitionStorage> mvStorage0 = createMvStorage(0);
        CompletableFuture<MvPartitionStorage> mvStorage1 = createMvStorage(1);
        CompletableFuture<MvPartitionStorage> mvStorage2 = createMvStorage(2);
        CompletableFuture<MvPartitionStorage> mvStorage3 = createMvStorage(3);
        CompletableFuture<MvPartitionStorage> mvStorage4 = createMvStorage(4);
        CompletableFuture<MvPartitionStorage> mvStorage5 = createMvStorage(5);

        assertThat(mvStorage0, willCompleteSuccessfully());
        assertThat(mvStorage1, willCompleteSuccessfully());
        assertThat(mvStorage2, willCompleteSuccessfully());
        assertThat(mvStorage3, willCompleteSuccessfully());
        assertThat(mvStorage4, willCompleteSuccessfully());
        assertThat(mvStorage5, willCompleteSuccessfully());

        assertThat(destroyMvStorage(1), willCompleteSuccessfully());
        assertThat(clearMvStorage(2), willCompleteSuccessfully());
        assertThat(startRebalanceMvStorage(3), willCompleteSuccessfully());

        assertThat(startRebalanceMvStorage(4), willCompleteSuccessfully());
        assertThat(abortRebalanceMvStorage(4), willCompleteSuccessfully());

        assertThat(startRebalanceMvStorage(4), willCompleteSuccessfully());
        assertThat(finishRebalanceMvStorage(4), willCompleteSuccessfully());

        CompletableFuture<List<MvPartitionStorage>> allForCloseOrDestroy = mvPartitionStorages.getAllForCloseOrDestroy();

        assertThat(allForCloseOrDestroy, willCompleteSuccessfully());

        // One less, since we destroyed 1 storage.
        assertThat(
                allForCloseOrDestroy.join(),
                containsInAnyOrder(mvStorage0.join(), mvStorage2.join(), mvStorage3.join(), mvStorage4.join(), mvStorage5.join())
        );

        // What happens if we try to perform operations on storages?
        assertThrowsWithCause(() -> createMvStorage(6), StorageException.class, "Storage is in the process of closing");
        assertThrowsWithCause(() -> destroyMvStorage(0), StorageException.class, "Storage does not exist");
        assertThrowsWithCause(() -> clearMvStorage(0), StorageException.class, "Storage does not exist");
        assertThrowsWithCause(() -> startRebalanceMvStorage(0), StorageException.class, "Storage does not exist");
        assertThrowsWithCause(() -> abortRebalanceMvStorage(0), StorageException.class, "Storage does not exist");
        assertThrowsWithCause(() -> finishRebalanceMvStorage(0), StorageException.class, "Storage does not exist");
    }

    @Test
    void testWaitOperationOnGetAllForCloseOrDestroy() {
        CompletableFuture<Void> createStorageOperationFuture = new CompletableFuture<>();
        CompletableFuture<Void> destroyStorageOperationFuture = new CompletableFuture<>();
        CompletableFuture<Void> clearStorageOperationFuture = new CompletableFuture<>();
        CompletableFuture<Void> startRebalanceStorageOperationFuture = new CompletableFuture<>();
        CompletableFuture<Void> abortRebalanceStorageOperationFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishRebalanceStorageOperationFuture = new CompletableFuture<>();

        CompletableFuture<?> create0StorageFuture = runAsync(() -> mvPartitionStorages.create(0, partId -> {
            assertThat(createStorageOperationFuture, willCompleteSuccessfully());

            return mock(MvPartitionStorage.class);
        }));

        assertThat(createMvStorage(1), willCompleteSuccessfully());
        assertThat(createMvStorage(2), willCompleteSuccessfully());
        assertThat(createMvStorage(3), willCompleteSuccessfully());
        assertThat(createMvStorage(4), willCompleteSuccessfully());
        assertThat(createMvStorage(5), willCompleteSuccessfully());

        CompletableFuture<Void> destroy1StorageFuture = mvPartitionStorages.destroy(1, storage -> destroyStorageOperationFuture);
        CompletableFuture<Void> clear2StorageFuture = mvPartitionStorages.clear(2, storage -> clearStorageOperationFuture);

        CompletableFuture<Void> startRebalance3StorageFuture = mvPartitionStorages.startRebalance(
                3,
                storage -> startRebalanceStorageOperationFuture
        );

        assertThat(startRebalanceMvStorage(4), willCompleteSuccessfully());
        assertThat(startRebalanceMvStorage(5), willCompleteSuccessfully());

        CompletableFuture<Void> abortRebalance4StorageFuture = mvPartitionStorages.abortRebalance(
                4,
                storage -> abortRebalanceStorageOperationFuture
        );

        CompletableFuture<Void> finishRebalance5StorageFuture = mvPartitionStorages.finishRebalance(
                5,
                storage -> finishRebalanceStorageOperationFuture
        );

        CompletableFuture<List<MvPartitionStorage>> allForCloseOrDestroyFuture = mvPartitionStorages.getAllForCloseOrDestroy();

        assertThat(allForCloseOrDestroyFuture, willTimeoutFast());

        // Let's finish creating the storage.
        createStorageOperationFuture.complete(null);

        assertThat(create0StorageFuture, willCompleteSuccessfully());
        assertThat(allForCloseOrDestroyFuture, willTimeoutFast());

        // Let's finish destroying the storage.
        destroyStorageOperationFuture.complete(null);

        assertThat(destroy1StorageFuture, willCompleteSuccessfully());
        assertThat(allForCloseOrDestroyFuture, willTimeoutFast());

        // Let's finish clearing the storage.
        clearStorageOperationFuture.complete(null);

        assertThat(clear2StorageFuture, willCompleteSuccessfully());
        assertThat(allForCloseOrDestroyFuture, willTimeoutFast());

        // Let's finish starting rebalance the storage.
        startRebalanceStorageOperationFuture.complete(null);

        assertThat(startRebalance3StorageFuture, willCompleteSuccessfully());
        assertThat(allForCloseOrDestroyFuture, willTimeoutFast());

        // Let's finish aborting rebalance the storage.
        abortRebalanceStorageOperationFuture.complete(null);

        assertThat(abortRebalance4StorageFuture, willCompleteSuccessfully());
        assertThat(allForCloseOrDestroyFuture, willTimeoutFast());

        // Let's finish finishing rebalance the storage.
        finishRebalanceStorageOperationFuture.complete(null);

        assertThat(finishRebalance5StorageFuture, willCompleteSuccessfully());

        assertThat(allForCloseOrDestroyFuture, willCompleteSuccessfully());
    }

    @Test
    void testAbortRebalanceBeforeFinishStartRebalance() {
        assertThat(createMvStorage(0), willCompleteSuccessfully());

        CompletableFuture<Void> rebalanceFuture = new CompletableFuture<>();

        CompletableFuture<Void> startRebalanceFuture = mvPartitionStorages.startRebalance(0, mvPartitionStorage -> rebalanceFuture);

        CompletableFuture<Void> startAbortRebalanceFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishAbortRebalanceFuture = new CompletableFuture<>();

        CompletableFuture<Void> abortRebalanceFuture = mvPartitionStorages.abortRebalance(0, mvPartitionStorage -> {
            startAbortRebalanceFuture.complete(null);

            return finishAbortRebalanceFuture;
        });

        // Make sure that the abortion of the rebalance does not start until the start of the rebalance is over.
        assertThat(startAbortRebalanceFuture, willTimeoutFast());

        // You can't abort rebalancing a second time.
        assertThrowsWithCause(() -> abortRebalanceMvStorage(0), StorageRebalanceException.class, "Rebalance abort is already planned");

        rebalanceFuture.complete(null);

        // Let's make sure that the rebalancing abortion will start only after the rebalancing start is completed.
        assertThat(startRebalanceFuture, willCompleteSuccessfully());
        assertThat(startAbortRebalanceFuture, willCompleteSuccessfully());
        assertThat(abortRebalanceFuture, willTimeoutFast());

        // Let's finish the rebalancing abortion.
        finishAbortRebalanceFuture.complete(null);

        assertThat(abortRebalanceFuture, willCompleteSuccessfully());
    }

    private MvPartitionStorage getMvStorage(int partitionId) {
        return mvPartitionStorages.get(partitionId);
    }

    private CompletableFuture<MvPartitionStorage> createMvStorage(int partitionId) {
        return mvPartitionStorages.create(partitionId, partId -> mock(MvPartitionStorage.class));
    }

    private CompletableFuture<Void> destroyMvStorage(int partitionId) {
        return mvPartitionStorages.destroy(partitionId, mvStorage -> nullCompletedFuture());
    }

    private CompletableFuture<Void> clearMvStorage(int partitionId) {
        return mvPartitionStorages.clear(partitionId, mvStorage -> nullCompletedFuture());
    }

    private CompletableFuture<Void> startRebalanceMvStorage(int partitionId) {
        return mvPartitionStorages.startRebalance(partitionId, mvStorage -> nullCompletedFuture());
    }

    private CompletableFuture<Void> abortRebalanceMvStorage(int partitionId) {
        return mvPartitionStorages.abortRebalance(partitionId, mvStorage -> nullCompletedFuture());
    }

    private CompletableFuture<Void> finishRebalanceMvStorage(int partitionId) {
        return mvPartitionStorages.finishRebalance(partitionId, mvStorage -> nullCompletedFuture());
    }

    private int getPartitionIdOutOfConfig() {
        return PARTITIONS;
    }
}
