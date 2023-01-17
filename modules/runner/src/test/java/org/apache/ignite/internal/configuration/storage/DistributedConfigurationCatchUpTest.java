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

package org.apache.ignite.internal.configuration.storage;

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.TestConfigurationChanger;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link DistributedConfigurationStorage}.
 */
public class DistributedConfigurationCatchUpTest {
    private final VaultManager vaultManager = new VaultManager(new InMemoryVaultService());

    /**
     * Before each.
     */
    @BeforeEach
    void start() {
        vaultManager.start();
    }

    /**
     * After each.
     */
    @AfterEach
    void stop() throws Exception {
        vaultManager.stop();
    }

    /**
     * Dummy configuration.
     */
    @ConfigurationRoot(rootName = "someKey", type = DISTRIBUTED)
    public static class DistributedTestConfigurationSchema {
        @Value(hasDefault = true)
        public final int foobar = 0;
    }

    /**
     * Tests that distributed configuration storage correctly picks up latest configuration MetaStorage revision
     * during recovery process.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMetaStorageRevisionDifferentFromConfigurationOnRestart() throws Exception {
        RootKey<DistributedTestConfiguration, DistributedTestView> rootKey = DistributedTestConfiguration.KEY;

        ConfigurationAsmGenerator cgen = new ConfigurationAsmGenerator();

        MetaStorageMockWrapper wrapper = new MetaStorageMockWrapper();

        DistributedConfigurationStorage storage = storage(wrapper);

        try {
            var changer = new TestConfigurationChanger(cgen, List.of(rootKey), Set.of(),
                    storage, Collections.emptyList(), Collections.emptyList());

            try {
                changer.start();

                ConfigurationSource source = source(
                        rootKey,
                        (DistributedTestChange change) -> change.changeFoobar(1)
                );

                CompletableFuture<Void> change = changer.change(source);

                change.get();
            } finally {
                changer.stop();
            }
        } finally {
            storage.close();
        }

        // Put a value to the configuration, so we start on non-empty vault.
        vaultManager.put(MetaStorageMockWrapper.TEST_KEY, new byte[]{4, 1, 2, 3, 4}).get();

        // This emulates a change in MetaStorage that is not related to the configuration.
        vaultManager.put(MetaStorageManagerImpl.APPLIED_REV, ByteUtils.longToBytes(2)).get();

        storage = storage(wrapper);

        try {

            var changer = new TestConfigurationChanger(cgen, List.of(rootKey), Set.of(),
                    storage, Collections.emptyList(), Collections.emptyList());

            try {
                changer.start();

                // Should return last configuration change, not last MetaStorage change.
                Long lastConfigurationChangeRevision = storage.lastRevision().get();

                // Should be one, because we only changed configuration once.
                assertEquals(1, lastConfigurationChangeRevision);
            } finally {
                changer.stop();
            }
        } finally {
            storage.close();
        }
    }

    private DistributedConfigurationStorage storage(MetaStorageMockWrapper wrapper) {
        return new DistributedConfigurationStorage(wrapper.metaStorageManager(), vaultManager);
    }

    /**
     * This class stores data for {@link MetaStorageManager}'s mock.
     */
    private static class MetaStorageMockWrapper {
        private static final String DISTRIBUTED_PREFIX = "dst-cfg.";

        /**
         * This and previous field are copy-pasted intentionally, so in case if something changes,
         * this test should fail and be reviewed and re-written.
         */
        private static final ByteArray MASTER_KEY = new ByteArray(DISTRIBUTED_PREFIX + "$master$key");

        private static final ByteArray TEST_KEY = new ByteArray(DISTRIBUTED_PREFIX + "someKey.foobar");

        /** MetaStorage mock. */
        private final MetaStorageManager mock;

        /** Captured MetaStorage listener. */
        private WatchListener lsnr;

        /** Current master key revision. */
        private long masterKeyRevision;

        private MetaStorageMockWrapper() {
            mock = mock(MetaStorageManager.class);

            setup();
        }

        private void setup() {
            // Returns current master key revision.
            when(mock.get(MASTER_KEY)).then(invocation -> {
                return CompletableFuture.completedFuture(new EntryImpl(MASTER_KEY.bytes(), null, masterKeyRevision, -1));
            });

            // On any invocation - trigger storage listener.
            when(mock.invoke(any(), anyCollection(), any()))
                    .then(invocation -> {
                        triggerStorageListener();

                        return CompletableFuture.completedFuture(true);
                    });

            when(mock.invoke(any(), any(Operation.class), any()))
                    .then(invocation -> {
                        triggerStorageListener();

                        return CompletableFuture.completedFuture(true);
                    });

            // This captures the listener.
            when(mock.registerPrefixWatch(any(), any())).then(invocation -> {
                lsnr = invocation.getArgument(1);

                return CompletableFuture.completedFuture(null);
            });
        }

        /**
         * Triggers MetaStorage listener incrementing master key revision.
         */
        private void triggerStorageListener() {
            EntryEvent entryEvent = new EntryEvent(null, new EntryImpl(MASTER_KEY.bytes(), null, ++masterKeyRevision, -1));
            lsnr.onUpdate(new WatchEvent(entryEvent));
        }

        private MetaStorageManager metaStorageManager() {
            return mock;
        }
    }

    private static <CHANGET> ConfigurationSource source(RootKey<?, ? super CHANGET> rootKey, Consumer<CHANGET> changer) {
        return new ConfigurationSource() {
            @Override
            public void descend(ConstructableTreeNode node) {
                ConfigurationSource changerSrc = new ConfigurationSource() {
                    @Override
                    public void descend(ConstructableTreeNode node) {
                        changer.accept((CHANGET) node);
                    }
                };

                node.construct(rootKey.key(), changerSrc, true);
            }
        };
    }
}
