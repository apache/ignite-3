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

import static org.apache.ignite.internal.catalog.CatalogTestUtils.TEST_DELAY_DURATION;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.spy;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.PartitionCountCalculator;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for catalog compaction unit testing. */
@ExtendWith(ExecutorServiceExtension.class)
abstract class AbstractCatalogCompactionTest extends BaseIgniteAbstractTest {
    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutor;

    final HybridClock clock = new HybridClockImpl();

    private StandaloneMetaStorageManager metastore;

    private ClockWaiter clockWaiter;

    ClockService clockService;

    CatalogManagerImpl catalogManager;

    @BeforeEach
    void setUp() {
        clockWaiter = new ClockWaiter("test-node", clock, scheduledExecutor);

        clockService = new TestClockService(clock, clockWaiter);

        catalogManager = spy(createCatalogManager("test-node"));
    }

    @AfterEach
    void cleanup() {
        List.of(catalogManager, clockWaiter, metastore).forEach(IgniteComponent::beforeNodeStop);
        assertThat(IgniteUtils.stopAsync(new ComponentContext(), catalogManager, clockWaiter, metastore), willCompleteSuccessfully());
    }

    /** Creates catalog manager. */
    private CatalogManagerImpl createCatalogManager(String nodeName) {
        metastore = StandaloneMetaStorageManager.create(nodeName);
        FailureProcessor failureProcessor = new NoOpFailureManager();
        CatalogManagerImpl manager = new CatalogManagerImpl(
                new UpdateLogImpl(metastore, failureProcessor),
                clockService,
                failureProcessor,
                () -> TEST_DELAY_DURATION,
                PartitionCountCalculator.fixedPartitionCountCalculator()
        );

        assertThat(startAsync(new ComponentContext(), metastore), willCompleteSuccessfully());
        assertThat(metastore.recoveryFinishedFuture(), willCompleteSuccessfully());

        assertThat(startAsync(new ComponentContext(), clockWaiter, manager), willCompleteSuccessfully());
        assertThat("Watches were not deployed", metastore.deployWatches(), willCompleteSuccessfully());
        assertThat("Catalog initialization", manager.catalogInitializationFuture(), willCompleteSuccessfully());

        return manager;
    }
}
