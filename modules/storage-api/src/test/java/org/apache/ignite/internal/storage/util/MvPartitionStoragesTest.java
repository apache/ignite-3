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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willFailFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

/**
 * Class for testing {@link MvPartitionStorages}.
 */
@ExtendWith(ConfigurationExtension.class)
public class MvPartitionStoragesTest {
    @InjectConfiguration
    private TableConfiguration tableConfig;

    private MvPartitionStorages<MvPartitionStorage> mvPartitionStorages;

    @BeforeEach
    void setUp() {
        mvPartitionStorages = new MvPartitionStorages(tableConfig.value());
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

        assertThrowsWithMessage(StorageException.class, () -> createMvStorage(1), "Storage is in process of being created");

        finishCreateMvStorageFuture.complete(null);

        assertThat(createMvStorageFuture, willCompleteSuccessfully());
    }

    @Test
    void testCreateError() {
        assertThrows(IllegalArgumentException.class, () -> createMvStorage(getPartitionIdOutOfConfig()));

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        assertThrowsWithMessage(StorageException.class, () -> createMvStorage(0), "Storage already exists");

        // What if there is an error during the operation?

        assertThat(mvPartitionStorages.create(2, partId -> {
            throw new RuntimeException("from test");
        }), willFailFast(RuntimeException.class));

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

        assertThrowsWithMessage(StorageException.class, () -> createMvStorage(0),
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

        MvPartitionStorage mvStorage = getMvStorage(0);

        CompletableFuture<Void> startErrorDestroyMvStorageFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishErrorDestroyMvStorageFuture = new CompletableFuture<>();

        CompletableFuture<?> errorDestroyMvStorageFuture = runAsync(() ->
                assertThat(mvPartitionStorages.destroy(0, mvStorage1 -> {
                    startErrorDestroyMvStorageFuture.complete(null);

                    return finishErrorDestroyMvStorageFuture;
                }), willCompleteSuccessfully())
        );

        assertThat(startErrorDestroyMvStorageFuture, willCompleteSuccessfully());

        CompletableFuture<MvPartitionStorage> errorReCreateMvStorageFuture = createMvStorage(0);

        finishErrorDestroyMvStorageFuture.completeExceptionally(new RuntimeException("from test"));

        assertThat(errorDestroyMvStorageFuture, willFailFast(RuntimeException.class));
        assertThat(errorReCreateMvStorageFuture, willFailFast(RuntimeException.class));

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

        assertThrowsWithMessage(StorageException.class, () -> destroyMvStorage(0), "Storage does not exist");
        assertThrowsWithMessage(StorageException.class, () -> clearMvStorage(0), "Storage does not exist");

        assertThrowsWithMessage(StorageRebalanceException.class, () -> startRebalanceMvStorage(0), "Storage does not exist");
        assertThrowsWithMessage(StorageRebalanceException.class, () -> abortRebalanceMvStorage(0), "Storage does not exist");
        assertThrowsWithMessage(StorageRebalanceException.class, () -> finishRebalanceMvStorage(0), "Storage does not exist");

        finishDestroyMvStorageFuture.complete(null);

        assertThat(destroyMvStorageFuture, willCompleteSuccessfully());

        assertNull(getMvStorage(0));

        assertThat(createMvStorage(0), willCompleteSuccessfully());
    }

    @Test
    void testDestroyError() {
        assertThrows(IllegalArgumentException.class, () -> destroyMvStorage(getPartitionIdOutOfConfig()));

        assertThrowsWithMessage(StorageException.class, () -> destroyMvStorage(0), "Storage does not exist");

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        // What if there is an error during the operation?

        assertThat(
                mvPartitionStorages.destroy(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willFailFast(RuntimeException.class)
        );

        assertNull(getMvStorage(0));
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

        assertThrowsWithMessage(StorageException.class, () -> destroyMvStorage(0), "Storage is in process of being cleaned up");
        assertThrowsWithMessage(StorageException.class, () -> clearMvStorage(0), "Storage is in process of being cleaned up");

        assertThrowsWithMessage(StorageRebalanceException.class, () -> startRebalanceMvStorage(0),
                "Storage is in process of being cleaned up");
        assertThrowsWithMessage(StorageRebalanceException.class, () -> abortRebalanceMvStorage(0),
                "Storage is in process of being cleaned up");
        assertThrowsWithMessage(StorageRebalanceException.class, () -> finishRebalanceMvStorage(0),
                "Storage is in process of being cleaned up");

        finishCleanupMvStorageFuture.complete(null);

        assertThat(cleanupMvStorageFuture, willCompleteSuccessfully());

        assertSame(mvStorage, getMvStorage(0));
    }

    @Test
    void testClearError() {
        assertThrows(IllegalArgumentException.class, () -> clearMvStorage(getPartitionIdOutOfConfig()));

        assertThrowsWithMessage(StorageException.class, () -> clearMvStorage(0), "Storage does not exist");

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        // What if there is an error during the operation?

        assertThat(
                mvPartitionStorages.clear(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willFailFast(RuntimeException.class)
        );

        assertNotNull(getMvStorage(0));
    }

    @Test
    void testStartRebalance() {
        assertThat(createMvStorage(0), willCompleteSuccessfully());

        CompletableFuture<Void> startStartRebalanceMvStorage = new CompletableFuture<>();
        CompletableFuture<Void> finishStartRebalanceMvStorage = new CompletableFuture<>();

        CompletableFuture<?> startRebalanceFuture = runAsync(() ->
                assertThat(mvPartitionStorages.startRebalace(0, mvStorage -> {
                    startStartRebalanceMvStorage.complete(null);

                    return finishStartRebalanceMvStorage;
                }), willCompleteSuccessfully())
        );

        assertThat(startStartRebalanceMvStorage, willCompleteSuccessfully());

        assertThrowsWithMessage(StorageRebalanceException.class, () -> startRebalanceMvStorage(0),
                "Storage in the process of starting a rebalance");
        assertThrowsWithMessage(StorageRebalanceException.class, () -> abortRebalanceMvStorage(0),
                "Storage in the process of starting a rebalance");
        assertThrowsWithMessage(StorageRebalanceException.class, () -> finishRebalanceMvStorage(0),
                "Storage in the process of starting a rebalance");

        assertThrowsWithMessage(StorageException.class, () -> destroyMvStorage(0), "Storage in the process of starting a rebalance");
        assertThrowsWithMessage(StorageException.class, () -> clearMvStorage(0), "Storage in the process of starting a rebalance");

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

        assertThrowsWithMessage(StorageRebalanceException.class, () -> startRebalanceMvStorage(0), "Storage does not exist");

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        // What if there is an error during the operation?

        assertThat(
                mvPartitionStorages.startRebalace(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willFailFast(RuntimeException.class)
        );

        assertNotNull(getMvStorage(0));

        // What if we restart the rebalance?
        assertThat(abortRebalanceMvStorage(0), willCompleteSuccessfully());

        assertThat(startRebalanceMvStorage(0), willCompleteSuccessfully());

        assertThrows(StorageRebalanceException.class, () -> startRebalanceMvStorage(0), "Storage in the process of rebalance");
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

        assertThrowsWithMessage(StorageRebalanceException.class, () -> startRebalanceMvStorage(0),
                "Storage in the process of aborting a rebalance");
        assertThrowsWithMessage(StorageRebalanceException.class, () -> abortRebalanceMvStorage(0),
                "Storage in the process of aborting a rebalance");
        assertThrowsWithMessage(StorageRebalanceException.class, () -> finishRebalanceMvStorage(0),
                "Storage in the process of aborting a rebalance");

        assertThrowsWithMessage(StorageException.class, () -> destroyMvStorage(0), "Storage in the process of aborting a rebalance");
        assertThrowsWithMessage(StorageException.class, () -> clearMvStorage(0), "Storage in the process of aborting a rebalance");

        finishAbortRebalanceFuture.complete(null);

        assertThat(abortRebalanceFuture, willCompleteSuccessfully());

        assertNotNull(getMvStorage(0));

        // What if the rebalancing didn't start?

        AtomicBoolean invokeAbortFunction = new AtomicBoolean();

        assertThat(mvPartitionStorages.abortRebalance(0, mvStorage -> {
            invokeAbortFunction.set(true);

            return completedFuture(null);
        }), willCompleteSuccessfully());

        assertFalse(invokeAbortFunction.get());

        // What if the start of the rebalance ended with an error?

        invokeAbortFunction.set(false);

        assertThat(
                mvPartitionStorages.startRebalace(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willFailFast(RuntimeException.class)
        );

        assertThat(mvPartitionStorages.abortRebalance(0, mvStorage -> {
            invokeAbortFunction.set(true);

            return completedFuture(null);
        }), willCompleteSuccessfully());

        assertTrue(invokeAbortFunction.get());
    }

    @Test
    void testAbortRebalanceError() {
        assertThrows(IllegalArgumentException.class, () -> abortRebalanceMvStorage(getPartitionIdOutOfConfig()));

        assertThrowsWithMessage(StorageRebalanceException.class, () -> abortRebalanceMvStorage(0), "Storage does not exist");

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        assertThat(startRebalanceMvStorage(0), willCompleteSuccessfully());

        // What if there is an error during the operation?

        assertThat(
                mvPartitionStorages.abortRebalance(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willFailFast(RuntimeException.class)
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

        assertThrowsWithMessage(StorageRebalanceException.class, () -> startRebalanceMvStorage(0),
                "Storage in the process of finishing a rebalance");
        assertThrowsWithMessage(StorageRebalanceException.class, () -> abortRebalanceMvStorage(0),
                "Storage in the process of finishing a rebalance");
        assertThrowsWithMessage(StorageRebalanceException.class, () -> finishRebalanceMvStorage(0),
                "Storage in the process of finishing a rebalance");

        assertThrowsWithMessage(StorageException.class, () -> destroyMvStorage(0), "Storage in the process of finishing a rebalance");
        assertThrowsWithMessage(StorageException.class, () -> clearMvStorage(0), "Storage in the process of finishing a rebalance");

        finishFinishRebalanceFuture.complete(null);

        assertThat(finishRebalanceFuture, willCompleteSuccessfully());

        assertNotNull(getMvStorage(0));
    }

    @Test
    void testFinishRebalanceError() {
        assertThrows(IllegalArgumentException.class, () -> finishRebalanceMvStorage(getPartitionIdOutOfConfig()));

        assertThrowsWithMessage(StorageRebalanceException.class, () -> finishRebalanceMvStorage(0), "Storage does not exist");

        assertThat(createMvStorage(0), willCompleteSuccessfully());

        assertThrowsWithMessage(StorageRebalanceException.class, () -> finishRebalanceMvStorage(0), "Storage rebalancing did not start");

        assertThat(startRebalanceMvStorage(0), willCompleteSuccessfully());

        // What if there is an error during the operation?

        assertThat(
                mvPartitionStorages.finishRebalance(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willFailFast(RuntimeException.class)
        );

        // What if the start of the rebalance fails?

        assertThat(abortRebalanceMvStorage(0), willCompleteSuccessfully());

        assertThat(
                mvPartitionStorages.startRebalace(0, mvStorage -> failedFuture(new RuntimeException("from test"))),
                willFailFast(RuntimeException.class)
        );

        assertThat(finishRebalanceMvStorage(0), willFailFast(RuntimeException.class));
    }

    @Test
    void testGetAllForClose() {
        MvPartitionStorage storage0 = mock(MvPartitionStorage.class);
        MvPartitionStorage storage1 = mock(MvPartitionStorage.class);

        assertThat(mvPartitionStorages.create(0, partId -> storage0), willCompleteSuccessfully());
        assertThat(mvPartitionStorages.create(1, partId -> storage1), willCompleteSuccessfully());

        assertThat(mvPartitionStorages.getAllForClose(), contains(storage0, storage1));
    }

    @Test
    void testDestroyAll() {
        MvPartitionStorage storage0 = mock(MvPartitionStorage.class);
        MvPartitionStorage storage1 = mock(MvPartitionStorage.class);

        assertThat(mvPartitionStorages.create(0, partId -> storage0), willCompleteSuccessfully());
        assertThat(mvPartitionStorages.create(1, partId -> storage1), willCompleteSuccessfully());

        CompletableFuture<Void> startDestroyMvStorage0Future = new CompletableFuture<>();
        CompletableFuture<Void> finishDestroyMvStorage0Future = new CompletableFuture<>();

        CompletableFuture<?> destroyMvStorage0Future = runAsync(() ->
                assertThat(mvPartitionStorages.destroy(0, mvStorage -> {
                    startDestroyMvStorage0Future.complete(null);

                    return finishDestroyMvStorage0Future;
                }), willCompleteSuccessfully())
        );

        assertThat(startDestroyMvStorage0Future, willCompleteSuccessfully());

        CompletableFuture<Void> destroyAllMvStoragesFuture = mvPartitionStorages.destroyAll(mvStorage -> {
            assertSame(mvStorage, storage1);

            return completedFuture(null);
        });

        assertThat(destroyAllMvStoragesFuture, willFailFast(TimeoutException.class));

        finishDestroyMvStorage0Future.complete(null);

        assertThat(destroyMvStorage0Future, willCompleteSuccessfully());
        assertThat(destroyAllMvStoragesFuture, willCompleteSuccessfully());
    }

    private MvPartitionStorage getMvStorage(int partitionId) {
        return mvPartitionStorages.get(partitionId);
    }

    private CompletableFuture<MvPartitionStorage> createMvStorage(int partitionId) {
        return mvPartitionStorages.create(partitionId, partId -> mock(MvPartitionStorage.class));
    }

    private CompletableFuture<Void> destroyMvStorage(int partitionId) {
        return mvPartitionStorages.destroy(partitionId, mvStorage -> completedFuture(null));
    }

    private CompletableFuture<Void> clearMvStorage(int partitionId) {
        return mvPartitionStorages.clear(partitionId, mvStorage -> completedFuture(null));
    }

    private CompletableFuture<Void> startRebalanceMvStorage(int partitionId) {
        return mvPartitionStorages.startRebalace(partitionId, mvStorage -> completedFuture(null));
    }

    private CompletableFuture<Void> abortRebalanceMvStorage(int partitionId) {
        return mvPartitionStorages.abortRebalance(partitionId, mvStorage -> completedFuture(null));
    }

    private CompletableFuture<Void> finishRebalanceMvStorage(int partitionId) {
        return mvPartitionStorages.finishRebalance(partitionId, mvStorage -> completedFuture(null));
    }

    private int getPartitionIdOutOfConfig() {
        return tableConfig.partitions().value();
    }

    private static <T extends Throwable> void assertThrowsWithMessage(
            Class<T> expectedType,
            Executable executable,
            String expectedErrorMessageSubString
    ) {
        T throwable = assertThrows(expectedType, executable);

        assertThat(throwable.getMessage(), containsString(expectedErrorMessageSubString));
    }
}
