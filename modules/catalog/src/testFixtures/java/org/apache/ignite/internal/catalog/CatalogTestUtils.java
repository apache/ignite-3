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

import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;

/**
 * Utilities for working with the catalog in tests.
 */
public class CatalogTestUtils {
    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals he needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     */
    public static CatalogManager createTestCatalogManager(String nodeName, HybridClock clock) {
        var vault = new VaultManager(new InMemoryVaultService());

        var metastore = StandaloneMetaStorageManager.create(vault, new SimpleInMemoryKeyValueStorage(nodeName));

        var clockWaiter = new ClockWaiter(nodeName, clock);

        return new CatalogManagerImpl(new UpdateLogImpl(metastore), clockWaiter) {
            @Override
            public void start() {
                vault.start();
                metastore.start();
                clockWaiter.start();

                super.start();

                assertThat(metastore.deployWatches(), willCompleteSuccessfully());
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
                metastore.beforeNodeStop();
                vault.beforeNodeStop();
            }

            @Override
            public void stop() throws Exception {
                super.stop();

                clockWaiter.stop();
                metastore.stop();
                vault.stop();
            }
        };
    }

    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals he needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     * @param metastore Meta storage manager.
     */
    public static CatalogManager createTestCatalogManager(String nodeName, HybridClock clock, MetaStorageManager metastore) {
        var clockWaiter = new ClockWaiter(nodeName, clock);

        return new CatalogManagerImpl(new UpdateLogImpl(metastore), clockWaiter) {
            @Override
            public void start() {
                clockWaiter.start();

                super.start();
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
            }

            @Override
            public void stop() throws Exception {
                super.stop();

                clockWaiter.stop();
            }
        };
    }
}
