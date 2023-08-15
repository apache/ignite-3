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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.spy;

import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for testing the {@link CatalogManager}.
 */
public abstract class BaseCatalogManagerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "test";

    final HybridClock clock = new HybridClockImpl();

    private VaultManager vault;

    private MetaStorageManager metastore;

    UpdateLog updateLog;

    ClockWaiter clockWaiter;

    protected CatalogManagerImpl manager;

    @BeforeEach
    void setUp() {
        vault = new VaultManager(new InMemoryVaultService());

        metastore = StandaloneMetaStorageManager.create(vault, new SimpleInMemoryKeyValueStorage(NODE_NAME));

        updateLog = spy(new UpdateLogImpl(metastore));
        clockWaiter = spy(new ClockWaiter(NODE_NAME, clock));

        manager = new CatalogManagerImpl(updateLog, clockWaiter);

        vault.start();
        metastore.start();
        clockWaiter.start();
        manager.start();

        assertThat("Watches were not deployed", metastore.deployWatches(), willCompleteSuccessfully());
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(Stream.of(manager, clockWaiter, metastore, vault)
                .filter(Objects::nonNull)
                .map(component -> component::stop)
        );
    }
}
