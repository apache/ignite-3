/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.TestConfigurationChanger;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.EntryEvent;
import org.apache.ignite.internal.metastorage.client.EntryImpl;
import org.apache.ignite.internal.metastorage.client.Operation;
import org.apache.ignite.internal.metastorage.client.SimpleCondition;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.metastorage.common.OperationType;
import org.apache.ignite.internal.metastorage.server.ExistenceCondition;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.RevisionCondition;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for the {@link DistributedConfigurationStorage}.
 */
public class DistributedConfigurationStorageTest extends ConfigurationStorageTest {
    private final VaultManager vaultManager = new VaultManager(new InMemoryVaultService());

    private final KeyValueStorage metaStorage = new SimpleInMemoryKeyValueStorage();

    private final MetaStorageManager metaStorageManager = mockMetaStorageManager();

    /**
     * Before each.
     */
    @BeforeEach
    void start() {
        vaultManager.start();
        metaStorage.start();
        metaStorageManager.start();
    }

    /**
     * After each.
     */
    @AfterEach
    void stop() throws Exception {
        metaStorageManager.stop();
        metaStorage.close();
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

        int configurationChangesCount = 7;

        try (var storage = new DistributedConfigurationStorage(wrapper.metaStorageManager(), vaultManager)) {
            var changer = new TestConfigurationChanger(cgen, List.of(rootKey), Collections.emptyMap(),
                    storage, Collections.emptyList(), Collections.emptyList());

            changer.start();

            for (int i = 0; i < configurationChangesCount; i++) {
                ConfigurationSource source = source(
                        rootKey,
                        (DistributedTestChange change) -> change.changeFoobar(1)
                );

                CompletableFuture<Void> change = changer.change(source);

                change.get();
            }

            changer.stop();
        }

        // Put a value to the configuration, so we start on non-empty vault.
        vaultManager.put(MetaStorageMockWrapper.TEST_KEY, new byte[]{4, 1, 2, 3, 4}).get();

        // This emulates a change in MetaStorage that is not related to the configuration.
        vaultManager.put(MetaStorageManager.APPLIED_REV, ByteUtils.longToBytes(configurationChangesCount + 1)).get();

        try (var storage = new DistributedConfigurationStorage(wrapper.metaStorageManager(), vaultManager)) {
            var changer = new TestConfigurationChanger(cgen, List.of(rootKey), Collections.emptyMap(),
                    storage, Collections.emptyList(), Collections.emptyList());

            changer.start();

            CompletableFuture<Long> longCompletableFuture = storage.lastRevision();

            // Should return last configuration change, not last MetaStorage change.
            Long lastConfigurationChangeRevision = longCompletableFuture.get();

            assertEquals(configurationChangesCount, lastConfigurationChangeRevision);

            changer.stop();
        }
    }

    /**
     * This class stores data for {@link MetaStorageManager}'s mock.
     */
    private static class MetaStorageMockWrapper {
        private static final String DISTRIBUTED_PREFIX = "dst-cfg.";

        /**
         * This and previous field are copy-pasted intentionally, so in case if something changes,
         * this test should fail and be reviewed and re-wrote.
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
                return CompletableFuture.completedFuture(new EntryImpl(MASTER_KEY, null, masterKeyRevision, -1));
            });

            // On any invocation - trigger storage listener.
            when(mock.invoke(any(), Mockito.<Collection<Operation>>any(), Mockito.any()))
                    .then(invocation -> {
                        triggerStorageListener();

                        return CompletableFuture.completedFuture(true);
                    });

            when(mock.invoke(any(), Mockito.<Operation>any(), Mockito.any()))
                    .then(invocation -> {
                        triggerStorageListener();

                        return CompletableFuture.completedFuture(true);
                    });

            // This captures the listener.
            when(mock.registerWatchByPrefix(any(), any())).then(invocation -> {
                lsnr = invocation.getArgument(1);

                return CompletableFuture.completedFuture(null);
            });
        }

        /**
         * Triggers MetaStorage listener incrementing master key revision.
         */
        private void triggerStorageListener() {
            EntryEvent entryEvent = new EntryEvent(null, new EntryImpl(MASTER_KEY, null, ++masterKeyRevision, -1));
            lsnr.onUpdate(new WatchEvent(entryEvent));
        }

        private MetaStorageManager metaStorageManager() {
            return mock;
        }
    }

    /** {@inheritDoc} */
    @Override
    public ConfigurationStorage getStorage() {
        return new DistributedConfigurationStorage(metaStorageManager, vaultManager);
    }

    /**
     * Creates a mock implementation of a {@link MetaStorageManager}.
     */
    private MetaStorageManager mockMetaStorageManager() {
        var mock = mock(MetaStorageManager.class);

        when(mock.invoke(any(), anyCollection(), anyCollection())).thenAnswer(invocation -> {
            SimpleCondition condition = invocation.getArgument(0);
            Collection<Operation> success = invocation.getArgument(1);
            Collection<Operation> failure = invocation.getArgument(2);

            boolean invokeResult = metaStorage.invoke(
                    toServerCondition(condition),
                    success.stream().map(DistributedConfigurationStorageTest::toServerOperation).collect(toList()),
                    failure.stream().map(DistributedConfigurationStorageTest::toServerOperation).collect(toList())
            );

            return CompletableFuture.completedFuture(invokeResult);
        });

        try {
            when(mock.range(any(), any())).thenAnswer(invocation -> {
                ByteArray keyFrom = invocation.getArgument(0);
                ByteArray keyTo = invocation.getArgument(1);

                return new CursorAdapter(metaStorage.range(keyFrom.bytes(), keyTo == null ? null : keyTo.bytes(), false));
            });
        } catch (NodeStoppingException e) {
            throw new RuntimeException(e);
        }

        return mock;
    }

    /**
     * Converts a {@link SimpleCondition} to a {@link org.apache.ignite.internal.metastorage.server.Condition}.
     */
    private static org.apache.ignite.internal.metastorage.server.Condition toServerCondition(SimpleCondition condition) {
        switch (condition.type()) {
            case REV_LESS_OR_EQUAL:
                return new RevisionCondition(
                        RevisionCondition.Type.LESS_OR_EQUAL,
                        condition.inner().key(),
                        ((SimpleCondition.RevisionCondition) condition.inner()).revision()
                );
            case KEY_NOT_EXISTS:
                return new ExistenceCondition(
                        ExistenceCondition.Type.NOT_EXISTS,
                        condition.inner().key()
                );
            default:
                throw new UnsupportedOperationException("Unsupported condition type: " + condition.type());
        }
    }

    /**
     * Converts a {@link Operation} to a {@link org.apache.ignite.internal.metastorage.server.Operation}.
     */
    private static org.apache.ignite.internal.metastorage.server.Operation toServerOperation(Operation operation) {
        switch (operation.type()) {
            case PUT:
                return new org.apache.ignite.internal.metastorage.server.Operation(
                        OperationType.PUT,
                        operation.inner().key(),
                        ((Operation.PutOp) (operation.inner())).value()
                );
            case REMOVE:
                return new org.apache.ignite.internal.metastorage.server.Operation(
                        OperationType.REMOVE,
                        operation.inner().key(),
                        null
                );
            case NO_OP:
                return new org.apache.ignite.internal.metastorage.server.Operation(
                        OperationType.NO_OP,
                        null,
                        null
                );
            default:
                throw new UnsupportedOperationException("Unsupported operation type: " + operation.type());
        }
    }

    /**
     * {@code Cursor} that converts {@link Entry} to {@link org.apache.ignite.internal.metastorage.server.Entry}.
     */
    private static class CursorAdapter implements Cursor<Entry> {
        /** Internal cursor. */
        private final Cursor<org.apache.ignite.internal.metastorage.server.Entry> internalCursor;

        /**
         * Constructor.
         *
         * @param internalCursor internal cursor.
         */
        CursorAdapter(Cursor<org.apache.ignite.internal.metastorage.server.Entry> internalCursor) {
            this.internalCursor = internalCursor;
        }

        /** {@inheritDoc} */
        @Override
        public void close() throws Exception {
            internalCursor.close();
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            return internalCursor.hasNext();
        }

        /** {@inheritDoc} */
        @Override
        public Entry next() {
            org.apache.ignite.internal.metastorage.server.Entry next = internalCursor.next();

            return new Entry() {
                @Override
                public @NotNull ByteArray key() {
                    return new ByteArray(next.key());
                }

                @Override
                public byte @Nullable [] value() {
                    return next.value();
                }

                @Override
                public long revision() {
                    return next.revision();
                }

                @Override
                public long updateCounter() {
                    return next.updateCounter();
                }

                @Override
                public boolean empty() {
                    return next.empty();
                }

                @Override
                public boolean tombstone() {
                    return next.tombstone();
                }
            };
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
