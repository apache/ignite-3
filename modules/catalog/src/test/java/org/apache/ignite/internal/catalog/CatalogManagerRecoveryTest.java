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
import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.TABLE_NAME;
import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.simpleIndex;
import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.simpleTable;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.TestRocksDbKeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** For {@link CatalogManager} testing on recovery. */
@ExtendWith(WorkDirectoryExtension.class)
public class CatalogManagerRecoveryTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "test-node-name";

    @WorkDirectory
    private Path workDir;

    private final HybridClock clock = new HybridClockImpl();

    private MetaStorageManager metaStorageManager;

    private CatalogManager catalogManager;

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.stopAll(catalogManager, metaStorageManager);
    }

    @Test
    void testRecoveryCatalogVersionTimestamps() throws Exception {
        createAndStartComponents();

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

        createAndStartComponents();

        // Let's check that the versions for the points in time at which they were created are in place.
        assertThat(catalogManager.activeCatalogVersion(time0), equalTo(catalogVersion0));
        assertThat(catalogManager.activeCatalogVersion(time1), equalTo(catalogVersion1));
    }

    private void createAndStartComponents() {
        createComponents();

        startComponentsAndDeployWatches();
    }

    private void createComponents() {
        KeyValueStorage keyValueStorage = new TestRocksDbKeyValueStorage(NODE_NAME, workDir);

        metaStorageManager = StandaloneMetaStorageManager.create(keyValueStorage);

        catalogManager = CatalogTestUtils.createTestCatalogManager(NODE_NAME, clock, metaStorageManager);
    }

    private void startComponentsAndDeployWatches() {
        Stream.of(metaStorageManager, catalogManager).forEach(IgniteComponent::start);

        assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());
    }

    private void stopComponents() throws Exception {
        IgniteUtils.stopAll(catalogManager, metaStorageManager);
    }
}
