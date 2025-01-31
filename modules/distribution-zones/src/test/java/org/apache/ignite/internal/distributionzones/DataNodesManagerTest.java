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

package org.apache.ignite.internal.distributionzones;

import static java.util.Collections.emptyList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link DataNodesManager}.
 */
@ExtendWith(ExecutorServiceExtension.class)
public class DataNodesManagerTest {
    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutor;

    private final String nodeName = "node";
    private final KeyValueStorage storage = new SimpleInMemoryKeyValueStorage(nodeName);
    private final HybridClock clock = new HybridClockImpl();
    private final ClockService clockService = new ClockServiceImpl(clock, new ClockWaiter(nodeName, clock, scheduledExecutor), () -> 0L);
    private final MetaStorageManager metaStorageManager = StandaloneMetaStorageManager
            .create(storage, clock, new ReadOperationForCompactionTracker());
    private final UpdateLog updateLog = new UpdateLogImpl(metaStorageManager);
    private final CatalogManager catalogManager = new CatalogManagerImpl(updateLog, clockService);
    private final DataNodesManager dataNodesManager =
            new DataNodesManager(nodeName, new IgniteSpinBusyLock(), metaStorageManager, catalogManager);

    public DataNodesManagerTest() {
        ComponentContext componentContext = new ComponentContext();

        assertThat(catalogManager.startAsync(componentContext), willCompleteSuccessfully());
        assertThat(metaStorageManager.startAsync(componentContext), willCompleteSuccessfully());
        dataNodesManager.start(emptyList());
    }

    @Test
    public void test() {

    }
}
