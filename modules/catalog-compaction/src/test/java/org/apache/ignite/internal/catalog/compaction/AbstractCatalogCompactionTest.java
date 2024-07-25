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

package org.apache.ignite.internal.catalog.compaction;

import static org.apache.ignite.internal.catalog.CatalogTestUtils.awaitDefaultZoneCreation;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;

/** Base class for catalog compaction unit testing. */
abstract class AbstractCatalogCompactionTest extends BaseIgniteAbstractTest {
    private final HybridClock clock = new HybridClockImpl();
    private final ClockWaiter clockWaiter = new ClockWaiter("test-node", clock);

    final ClockService clockService = new TestClockService(clock, clockWaiter);

    CatalogManagerImpl catalogManager;

    @BeforeEach
    void setUp() {
        catalogManager = createCatalogManager("test-node");
    }

    /** Creates catalog manager. */
    private CatalogManagerImpl createCatalogManager(String nodeName) {
        StandaloneMetaStorageManager metastore = StandaloneMetaStorageManager.create(new SimpleInMemoryKeyValueStorage(nodeName));
        CatalogManagerImpl manager = new CatalogManagerImpl(new UpdateLogImpl(metastore), clockService);

        assertThat(startAsync(new ComponentContext(), metastore, clockWaiter, manager), willCompleteSuccessfully());
        assertThat("Watches were not deployed", metastore.deployWatches(), willCompleteSuccessfully());
        awaitDefaultZoneCreation(manager);

        return manager;
    }
}
