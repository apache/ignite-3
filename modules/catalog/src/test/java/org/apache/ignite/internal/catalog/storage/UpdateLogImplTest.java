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

package org.apache.ignite.internal.catalog.storage;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.VersionedUpdateSerializers.VersionedUpdateSerializerV1;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.catalog.storage.serialization.UpdateLogMarshallerImpl;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ByteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests to verify {@link UpdateLogImpl}. */
class UpdateLogImplTest extends BaseIgniteAbstractTest {
    private KeyValueStorage keyValueStorage;

    private MetaStorageManager metastore;

    private final ReadOperationForCompactionTracker readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

    @BeforeEach
    void setUp() {
        keyValueStorage = new SimpleInMemoryKeyValueStorage("test", readOperationForCompactionTracker);

        metastore = StandaloneMetaStorageManager.create(keyValueStorage, readOperationForCompactionTracker);

        keyValueStorage.start();
        assertThat(metastore.startAsync(new ComponentContext()), willCompleteSuccessfully());
        assertThat(metastore.recoveryFinishedFuture(), willCompleteSuccessfully());
    }

    @AfterEach
    public void tearDown() throws Exception {
        closeAll(
                metastore == null ? null : () -> assertThat(metastore.stopAsync(new ComponentContext()), willCompleteSuccessfully())
        );
    }

    @Test
    void logReplayedOnStart() throws Exception {
        // First, let's append a few entries to the update log.
        UpdateLogImpl updateLogImpl = createAndStartUpdateLogImpl((update, ts, causalityToken) -> nullCompletedFuture());

        deployWatchesAndAwaitInitialUpdates();

        List<VersionedUpdate> expectedUpdates = List.of(singleEntryUpdateOfVersion(1), singleEntryUpdateOfVersion(2));

        appendUpdates(updateLogImpl, expectedUpdates);

        // Let's restart the log and metastore with recovery.
        assertThat(updateLogImpl.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        restartMetastore();

        var actualUpdates = new ArrayList<UpdateLogEvent>();

        createAndStartUpdateLogImpl((update, ts, causalityToken) -> {
            actualUpdates.add(update);

            return nullCompletedFuture();
        });

        // Let's check that we have recovered to the latest version.
        assertThat(actualUpdates, equalTo(expectedUpdates));
    }

    @Test
    void snapshotAppliedOnStart() throws Exception {
        // First, let's append a few entries to the update log.
        UpdateLogImpl updateLogImpl = createAndStartUpdateLogImpl((update, ts, causalityToken) -> nullCompletedFuture());

        deployWatchesAndAwaitInitialUpdates();

        List<VersionedUpdate> updates = List.of(
                singleEntryUpdateOfVersion(1),
                singleEntryUpdateOfVersion(2),
                singleEntryUpdateOfVersion(3)
        );

        appendUpdates(updateLogImpl, updates);

        compactCatalog(updateLogImpl, snapshotEntryOfVersion(2));

        // Let's restart the log and metastore with recovery.
        assertThat(updateLogImpl.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        restartMetastore();

        var actualUpdates = new ArrayList<UpdateLogEvent>();

        createAndStartUpdateLogImpl((update, ts, causalityToken) -> {
            actualUpdates.add(update);

            return nullCompletedFuture();
        });

        List<UpdateLogEvent> expectedUpdates = List.of(
                snapshotEntryOfVersion(2),
                singleEntryUpdateOfVersion(3)
        );

        // Let's check that we have recovered to the latest version.
        assertThat(actualUpdates, equalTo(expectedUpdates));
    }

    @Test
    public void saveSnapshotMultipleTimesToAdvanceItsVersion() throws Exception {
        UpdateLogImpl updateLogImpl = createAndStartUpdateLogImpl((update, ts, causalityToken) -> nullCompletedFuture());

        deployWatchesAndAwaitInitialUpdates();

        {
            List<VersionedUpdate> updates = List.of(
                    singleEntryUpdateOfVersion(1),
                    singleEntryUpdateOfVersion(2),
                    singleEntryUpdateOfVersion(3)
            );

            appendUpdates(updateLogImpl, updates);

            compactCatalog(updateLogImpl, snapshotEntryOfVersion(2));
        }

        {
            List<VersionedUpdate> updates = List.of(
                    singleEntryUpdateOfVersion(4),
                    singleEntryUpdateOfVersion(5)
            );

            appendUpdates(updateLogImpl, updates);

            compactCatalog(updateLogImpl, snapshotEntryOfVersion(4));
        }
    }

    private void compactCatalog(UpdateLogImpl updateLogImpl, SnapshotEntry update) throws InterruptedException {
        long revisionBeforeAppend = metastore.appliedRevision();
        assertThat(updateLogImpl.saveSnapshot(update), willCompleteSuccessfully());
        assertTrue(waitForCondition(
                () -> metastore.appliedRevision() == revisionBeforeAppend + 1,
                TimeUnit.SECONDS.toMillis(1))
        );
    }

    private UpdateLogImpl createUpdateLogImpl() {
        return new UpdateLogImpl(metastore, new NoOpFailureManager(), new UpdateLogMarshallerImpl(new TestEntrySerializerProvider()));
    }

    private UpdateLogImpl createAndStartUpdateLogImpl(OnUpdateHandler onUpdateHandler) {
        UpdateLogImpl updateLogImpl = createUpdateLogImpl();

        updateLogImpl.registerUpdateHandler(onUpdateHandler);
        assertThat(updateLogImpl.startAsync(new ComponentContext()), willCompleteSuccessfully());

        return updateLogImpl;
    }

    private void appendUpdates(UpdateLogImpl updateLogImpl, Collection<VersionedUpdate> updates) throws Exception {
        long revisionBeforeAppend = metastore.appliedRevision();

        updates.forEach(update -> assertThat(updateLogImpl.append(update), willBe(true)));

        assertTrue(waitForCondition(
                () -> metastore.appliedRevision() - updates.size() == revisionBeforeAppend,
                TimeUnit.SECONDS.toMillis(1))
        );
    }

    private void restartMetastore() {
        long recoverRevision = metastore.appliedRevision();

        ComponentContext componentContext = new ComponentContext();

        assertThat(metastore.stopAsync(componentContext), willCompleteSuccessfully());

        metastore = StandaloneMetaStorageManager.create(keyValueStorage, readOperationForCompactionTracker);
        assertThat(metastore.startAsync(componentContext), willCompleteSuccessfully());

        assertThat(metastore.recoveryFinishedFuture().thenApply(Revisions::revision), willBe(recoverRevision));
    }

    @Test
    public void exceptionIsThrownOnStartIfHandlerHasNotBeenRegistered() {
        UpdateLogImpl updateLog = createUpdateLogImpl();

        IgniteInternalException ex = assertThrows(
                IgniteInternalException.class,
                () -> updateLog.startAsync(new ComponentContext())
        );

        assertThat(
                ex.getMessage(),
                containsString("Handler must be registered prior to component start")
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 8})
    public void appendAcceptsUpdatesInOrder(int startVersion) throws Exception {
        UpdateLogImpl updateLog = createUpdateLogImpl();

        List<Integer> appliedVersions = new ArrayList<>();
        List<Long> causalityTokens = new ArrayList<>();

        updateLog.registerUpdateHandler((update, ts, causalityToken) -> {
            appliedVersions.add(((VersionedUpdate) update).version() + 1);
            causalityTokens.add(causalityToken);

            return nullCompletedFuture();
        });

        long revisionBefore = metastore.appliedRevision();

        assertThat(updateLog.startAsync(new ComponentContext()), willCompleteSuccessfully());

        deployWatchesAndAwaitInitialUpdates();

        // first update should always be successful
        assertThat(updateLog.append(singleEntryUpdateOfVersion(startVersion)), willBe(true));

        // update of the same version should not be accepted
        assertThat(updateLog.append(singleEntryUpdateOfVersion(startVersion)), willBe(false));

        // update of the version lower than the last applied should not be accepted
        assertThat(updateLog.append(singleEntryUpdateOfVersion(startVersion - 1)), willBe(false));

        // update of the version creating gap should not be accepted
        assertThat(updateLog.append(singleEntryUpdateOfVersion(startVersion + 2)), willBe(false));

        // regular update of next version should be accepted
        assertThat(updateLog.append(singleEntryUpdateOfVersion(startVersion + 1)), willBe(true));

        // now the gap is filled, thus update should be accepted as well
        assertThat(updateLog.append(singleEntryUpdateOfVersion(startVersion + 2)), willBe(true));

        List<Integer> expectedVersions = List.of(startVersion + 1, startVersion + 2, startVersion + 3);
        List<Long> expectedTokens = List.of(revisionBefore + 2, revisionBefore + 6, revisionBefore + 7);

        // No-op operation also increases the revision.
        int revisionsUpdateCount = 7;
        // wait till necessary revision is applied
        assertTrue(
                waitForCondition(
                        () -> metastore.appliedRevision() - revisionsUpdateCount == revisionBefore,
                        TimeUnit.SECONDS.toMillis(5)
                )
        );

        assertThat(appliedVersions, equalTo(expectedVersions));
        assertThat(causalityTokens, equalTo(expectedTokens));
    }

    @Test
    void testUpdateMetastoreRevisionAfterUpdateHandlerComplete() throws Exception {
        CompletableFuture<Void> onUpdateHandlerFuture = new CompletableFuture<>();

        UpdateLog updateLog = createAndStartUpdateLogImpl((update, metaStorageUpdateTimestamp, causalityToken) -> onUpdateHandlerFuture);

        deployWatchesAndAwaitInitialUpdates();

        long metastoreRevision = metastore.appliedRevision();

        assertThat(updateLog.append(singleEntryUpdateOfVersion(1)), willCompleteSuccessfully());

        // Let's make sure that the metastore revision will not increase until onUpdateHandlerFuture is completed.
        assertFalse(waitForCondition(() -> metastore.appliedRevision() > metastoreRevision, 200));

        // Let's make sure that the metastore revision increases after completing onUpdateHandlerFuture.
        onUpdateHandlerFuture.complete(null);

        assertTrue(waitForCondition(() -> metastore.appliedRevision() > metastoreRevision, 200));
    }

    @Test
    void testOnUpdateHandlerUsesCausalityTokenFromMetastore() throws Exception {
        CompletableFuture<Void> onUpdateHandlerFuture = new CompletableFuture<>();

        AtomicLong causalityTokenFromHandler = new AtomicLong(-1L);

        UpdateLog updateLog = createAndStartUpdateLogImpl((update, metaStorageUpdateTimestamp, causalityToken) -> {
            causalityTokenFromHandler.set(causalityToken);

            return onUpdateHandlerFuture;
        });

        deployWatchesAndAwaitInitialUpdates();

        long metastoreRevision = metastore.appliedRevision();

        assertThat(updateLog.append(singleEntryUpdateOfVersion(1)), willCompleteSuccessfully());

        // Let's make sure that the metastore revision will not increase until onUpdateHandlerFuture is completed.
        assertFalse(waitForCondition(() -> metastore.appliedRevision() > metastoreRevision, 200));

        // Let's make sure that the metastore revision increases after completing onUpdateHandlerFuture.
        onUpdateHandlerFuture.complete(null);

        // Assert that causality token from OnUpdateHandler is the same as the revision of this update in metastorage.
        assertTrue(waitForCondition(() -> metastore.appliedRevision() == causalityTokenFromHandler.get(), 200));
    }

    @Test
    void writeProductKeyOnStartup() throws InterruptedException {
        ByteArray key = ByteArray.fromString("catalog.product");
        byte[] modifiedValue;

        {
            UpdateLogImpl updateLogImpl = createAndStartUpdateLogImpl((update, ts, causalityToken) -> nullCompletedFuture());

            deployWatchesAndAwaitInitialUpdates();

            Entry defaultKeyEntry = metastore.get(key).join();
            assertFalse(defaultKeyEntry.empty());

            byte[] initialValue = defaultKeyEntry.value();
            assertNotNull(initialValue);

            // Update value
            modifiedValue = Arrays.copyOf(initialValue, initialValue.length);
            modifiedValue[0] = (byte) (modifiedValue[0] + 1);
            metastore.put(key, modifiedValue).join();

            // Stop the log
            updateLogImpl.stopAsync().join();
        }

        {
            // Restart the log
            UpdateLogImpl updateLogImpl = createAndStartUpdateLogImpl((update, ts, causalityToken) -> nullCompletedFuture());
            assertNotNull(updateLogImpl);

            // Expect the key has not been overwritten with default value,
            Entry defaultKeyEntry = metastore.get(key).join();
            assertFalse(defaultKeyEntry.empty());
            assertArrayEquals(modifiedValue, defaultKeyEntry.value());
        }
    }

    private static VersionedUpdate singleEntryUpdateOfVersion(int version) {
        return new VersionedUpdate(version, 1, List.of(new TestUpdateEntry("foo_" + version)));
    }

    private static SnapshotEntry snapshotEntryOfVersion(int version) {
        Catalog catalog = mock(Catalog.class);

        when(catalog.version()).thenReturn(version);
        when(catalog.defaultZone()).thenReturn(mock(CatalogZoneDescriptor.class));

        return new SnapshotEntry(catalog);
    }

    static class TestUpdateEntry implements UpdateEntry, Serializable {
        private static final long serialVersionUID = 4865078624964906600L;

        final String payload;

        TestUpdateEntry(String payload) {
            this.payload = payload;
        }

        @Override
        public Catalog applyUpdate(Catalog catalog, HybridTimestamp timestamp) {
            return catalog;
        }

        @Override
        public int typeId() {
            return -1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestUpdateEntry that = (TestUpdateEntry) o;

            return payload.equals(that.payload);
        }

        @Override
        public int hashCode() {
            return payload.hashCode();
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    private void deployWatchesAndAwaitInitialUpdates() throws InterruptedException {
        assertThat("Watches were not deployed", metastore.deployWatches(), willCompleteSuccessfully());

        // Wait until initial updates are applied.
        assertTrue(waitForCondition(() -> metastore.appliedRevision() > 0, 500));
    }

    private static class TestEntrySerializerProvider implements CatalogEntrySerializerProvider {
        @Override
        public CatalogObjectSerializer<MarshallableEntry> get(int versionNotUsed, int id) {
            CatalogObjectSerializer<? extends MarshallableEntry> serializer;

            if (id < 0) {
                serializer = new CatalogObjectSerializer<>() {
                    @Override
                    public MarshallableEntry readFrom(CatalogObjectDataInput input) throws IOException {
                        int length = input.readVarIntAsInt();
                        byte[] data = input.readByteArray(length);

                        return ByteUtils.fromBytes(data);
                    }

                    @Override
                    public void writeTo(MarshallableEntry value, CatalogObjectDataOutput output) throws IOException {
                        byte[] bytes = ByteUtils.toBytes(value);

                        output.writeVarInt(bytes.length);
                        output.writeByteArray(bytes);
                    }
                };
            } else if (id == MarshallableEntryType.VERSIONED_UPDATE.id()) {
                serializer = new VersionedUpdateSerializerV1(this);
            } else {
                serializer = DEFAULT_PROVIDER.get(2, id);
            }

            return (CatalogObjectSerializer<MarshallableEntry>) serializer;
        }

        @Override
        public int latestSerializerVersion(int typeId) {
            return 2;
        }
    }
}
