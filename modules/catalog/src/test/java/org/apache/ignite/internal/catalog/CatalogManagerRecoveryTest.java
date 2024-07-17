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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.INDEX_NAME;
import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.INDEX_NAME_2;
import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.TABLE_NAME;
import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.TABLE_NAME_2;
import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.TABLE_NAME_3;
import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.dropIndexCommand;
import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.dropTableCommand;
import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.simpleIndex;
import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.simpleTable;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.CatalogTestUtils.TestUpdateHandlerInterceptor;
import org.apache.ignite.internal.catalog.storage.SnapshotEntry;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.TestRocksDbKeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/** For {@link CatalogManager} testing on recovery. */
@ExtendWith(WorkDirectoryExtension.class)
public class CatalogManagerRecoveryTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "test-node-name";

    @WorkDirectory
    private Path workDir;

    private final HybridClock clock = new HybridClockImpl();

    private MetaStorageManager metaStorageManager;

    private CatalogManager catalogManager;

    private TestUpdateHandlerInterceptor interceptor;

    @AfterEach
    void tearDown() {
        assertThat(stopAsync(new ComponentContext(), catalogManager, metaStorageManager), willCompleteSuccessfully());
    }

    @Test
    void testRecoveryCatalogVersionTimestamps() throws InterruptedException {
        createAndStartComponents();
        awaitDefaultZoneCreation();

        // on the first start default zone must be created
        verify(metaStorageManager).invoke(any());

        // Let's create a couple of versions of the catalog.
        assertThat(catalogManager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        int catalogVersion0 = catalogManager.latestCatalogVersion();

        assertThat(catalogManager.execute(simpleIndex(TABLE_NAME, INDEX_NAME)), willCompleteSuccessfully());

        int catalogVersion1 = catalogManager.latestCatalogVersion();

        assertThat(catalogVersion1, greaterThan(catalogVersion0));

        long time0 = catalogManager.catalog(catalogVersion0).time();
        long time1 = catalogManager.catalog(catalogVersion1).time();

        assertThat(time1, greaterThan(time0));

        // We will restart and recover the components and also set the clock to the future.
        stopComponents();

        HybridTimestamp updateNow = clock.now().addPhysicalTime(TimeUnit.DAYS.toMillis(1));
        clock.update(updateNow);

        HybridTimestamp newNow = clock.now();
        assertThat(newNow, greaterThan(updateNow));

        reset(metaStorageManager);

        createAndStartComponents();

        // Check recovery events.
        verify(interceptor, times(3)).handle(any(VersionedUpdate.class), any(), anyLong());
        verify(interceptor, Mockito.never()).handle(any(SnapshotEntry.class), any(), anyLong());
        // on recovery no additional invocation should happen
        verify(metaStorageManager, Mockito.never()).invoke(any());


        // Let's check that the versions for the points in time at which they were created are in place.
        assertThat(catalogManager.activeCatalogVersion(time0), equalTo(catalogVersion0));
        assertThat(catalogManager.activeCatalogVersion(time1), equalTo(catalogVersion1));
    }

    @Test
    void testRecoveryCatalogAfterCompaction() throws InterruptedException {
        createAndStartComponents();
        awaitDefaultZoneCreation();

        // Let's create a couple of versions of the catalog.
        assertThat(catalogManager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(catalogManager.execute(simpleIndex(TABLE_NAME, INDEX_NAME)), willCompleteSuccessfully());
        assertThat(catalogManager.execute(simpleTable(TABLE_NAME_2)), willCompleteSuccessfully());
        assertThat(catalogManager.execute(simpleIndex(TABLE_NAME, INDEX_NAME_2)), willCompleteSuccessfully());
        assertThat(catalogManager.execute(dropTableCommand(TABLE_NAME_2)), willCompleteSuccessfully());
        assertThat(catalogManager.execute(dropIndexCommand(INDEX_NAME_2)), willCompleteSuccessfully());

        // Save version, which will be earliest version after snapshot.
        int earliestVersion = catalogManager.latestCatalogVersion();
        long earliestVersionActivationTime = catalogManager.catalog(earliestVersion).time();
        long snapshotTime = clock.nowLong();

        assertThat(catalogManager.execute(simpleTable(TABLE_NAME_3)), willCompleteSuccessfully());

        int latestVersion = catalogManager.latestCatalogVersion();
        long latestVersionActivationTime = catalogManager.catalog(latestVersion).time();

        // Compact catalog with ignoring snapshot event, then ensure earliest version wasn't changed
        clearInvocations(interceptor);
        interceptor.dropSnapshotEvents();

        assertThat(((CatalogManagerImpl) catalogManager).compactCatalog(snapshotTime), willBe(true));

        verify(interceptor, timeout(2_000)).handle(any(SnapshotEntry.class), any(), anyLong());
        assertThat(catalogManager.earliestCatalogVersion(), equalTo(0));

        // We will restart and recover the components and also set the clock to the future.
        stopComponents();

        createAndStartComponents();

        // Check recovery events.
        verify(interceptor).handle(any(SnapshotEntry.class), any(), anyLong());
        verify(interceptor).handle(any(VersionedUpdate.class), any(), anyLong());

        // Let's check Catalog was recovered from a snapshot and history starts from the expected version.
        assertThat(catalogManager.earliestCatalogVersion(), equalTo(earliestVersion));
        assertThrows(IllegalStateException.class, () -> catalogManager.activeCatalogVersion(0));
        assertThrows(IllegalStateException.class, () -> catalogManager.activeCatalogVersion(earliestVersionActivationTime - 1));
        assertThat(catalogManager.activeCatalogVersion(earliestVersionActivationTime), equalTo(earliestVersion));
        assertThat(catalogManager.activeCatalogVersion(snapshotTime), equalTo(earliestVersion));
        assertThat(catalogManager.activeCatalogVersion(latestVersionActivationTime), equalTo(latestVersion));
    }

    private void createAndStartComponents() {
        createComponents();

        startComponentsAndDeployWatches();
    }

    private void createComponents() {
        KeyValueStorage keyValueStorage = new TestRocksDbKeyValueStorage(NODE_NAME, workDir);

        metaStorageManager = spy(StandaloneMetaStorageManager.create(keyValueStorage));

        interceptor = spy(new TestUpdateHandlerInterceptor());

        catalogManager = CatalogTestUtils.createTestCatalogManagerWithInterceptor(NODE_NAME, clock, metaStorageManager, interceptor);
    }

    private void awaitDefaultZoneCreation() throws InterruptedException {
        waitForCondition(() -> catalogManager.latestCatalogVersion() > 0, 5_000);
    }

    private void startComponentsAndDeployWatches() {
        assertThat(
                startAsync(new ComponentContext(), metaStorageManager, catalogManager)
                        .thenCompose(unused -> metaStorageManager.deployWatches()),
                willCompleteSuccessfully()
        );
    }

    private void stopComponents() {
        assertThat(stopAsync(new ComponentContext(), catalogManager, metaStorageManager), willCompleteSuccessfully());
    }
}
