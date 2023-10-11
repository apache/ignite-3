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

package org.apache.ignite.internal.index;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link IndexAvailabilityController} testing. */
public class IndexAvailabilityControllerTest extends BaseIgniteAbstractTest {
    private static final String TABLE_NAME = "test-table";

    private static final String COLUMN_NAME = "test-column";

    private static final String INDEX_NAME = "test-index";
    private final HybridClock clock = new HybridClockImpl();

    private int partitions;

    private VaultManager vaultManager;

    private MetaStorageManagerImpl metaStorageManager;

    private CatalogManager catalogManager;

    private IndexAvailabilityController indexAvailabilityController;

    @BeforeEach
    void setUp() {
        vaultManager = new VaultManager(new InMemoryVaultService());

        metaStorageManager = StandaloneMetaStorageManager.create(vaultManager);

        catalogManager = CatalogTestUtils.createTestCatalogManager("test", clock, metaStorageManager);

        indexAvailabilityController = new IndexAvailabilityController(catalogManager, metaStorageManager);

        Stream.of(vaultManager, metaStorageManager, catalogManager, indexAvailabilityController).forEach(IgniteComponent::start);

        assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());

        CatalogZoneDescriptor zoneDescriptor = catalogManager.zone(DEFAULT_ZONE_NAME, clock.nowLong());

        assertNotNull(zoneDescriptor);

        partitions = zoneDescriptor.partitions();

        assertThat(partitions, greaterThan(1));

        TableTestUtils.createTable(
                catalogManager,
                DEFAULT_SCHEMA_NAME,
                DEFAULT_ZONE_NAME,
                TABLE_NAME,
                List.of(ColumnParams.builder().name(COLUMN_NAME).type(INT32).build()),
                List.of(COLUMN_NAME)
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.stopAll(indexAvailabilityController, catalogManager, metaStorageManager, vaultManager);
    }

    @Test
    void testMetastoreKeysAfterIndexCreate() throws Exception {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        awaitActualMetastoreRevision();

        assertThat(
                metaStorageManager.get(ByteArray.fromString(startBuildIndexKey(indexId))).thenApply(Entry::value),
                willBe(BYTE_EMPTY_ARRAY)
        );

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            String keyStr = partitionBuildIndexKey(indexId, partitionId);

            assertThat(keyStr, metaStorageManager.get(ByteArray.fromString(keyStr)).thenApply(Entry::value), willBe(BYTE_EMPTY_ARRAY));
        }
    }

    @Test
    void testMetastoreKeysAfterFinishBuildIndexForOnePartition() throws Exception {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        assertThat(metaStorageManager.remove(ByteArray.fromString(partitionBuildIndexKey(indexId, 0))), willCompleteSuccessfully());

        awaitActualMetastoreRevision();

        assertThat(
                metaStorageManager.get(ByteArray.fromString(startBuildIndexKey(indexId))).thenApply(Entry::value),
                willBe(BYTE_EMPTY_ARRAY)
        );

        assertThat(
                metaStorageManager.get(ByteArray.fromString(partitionBuildIndexKey(indexId, 0))).thenApply(Entry::value),
                willBe(nullValue())
        );

        for (int partitionId = 1; partitionId < partitions; partitionId++) {
            String keyStr = partitionBuildIndexKey(indexId, partitionId);

            assertThat(keyStr, metaStorageManager.get(ByteArray.fromString(keyStr)).thenApply(Entry::value), willBe(BYTE_EMPTY_ARRAY));
        }

        // TODO: IGNITE-19276 проверить что индекс в каталоге !НЕ! поменял состоянеие
    }

    @Test
    void testMetastoreKeysAfterFinishBuildIndexForAllPartition() throws Exception {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            assertThat(
                    metaStorageManager.remove(ByteArray.fromString(partitionBuildIndexKey(indexId, partitionId))),
                    willCompleteSuccessfully()
            );
        }

        awaitActualMetastoreRevision();

        assertThat(
                metaStorageManager.get(ByteArray.fromString(startBuildIndexKey(indexId))).thenApply(Entry::value),
                willBe(nullValue())
        );

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            String keyStr = partitionBuildIndexKey(indexId, partitionId);

            assertThat(keyStr, metaStorageManager.get(ByteArray.fromString(keyStr)).thenApply(Entry::value), willBe(nullValue()));
        }

        // TODO: IGNITE-19276 проверить что индекс в каталоге поменял состоянеие
    }

    private void awaitActualMetastoreRevision() throws Exception {
        assertTrue(
                waitForCondition(() -> {
                    CompletableFuture<Long> currentRevisionFuture = metaStorageManager.getService().currentRevision();

                    assertThat(currentRevisionFuture, willCompleteSuccessfully());

                    return currentRevisionFuture.join() == metaStorageManager.appliedRevision();
                }, 1_000)
        );
    }

    private void createIndex(String indexName) {
        TableTestUtils.createHashIndex(catalogManager, DEFAULT_SCHEMA_NAME, TABLE_NAME, indexName, List.of(COLUMN_NAME), false);
    }

    private int indexId(String indexName) {
        return TableTestUtils.getIndexIdStrict(catalogManager, indexName, clock.nowLong());
    }

    private static String startBuildIndexKey(int indexId) {
        return "startBuildIndex." + indexId;
    }

    private static String partitionBuildIndexKey(int indexId, int partitionId) {
        return "partitionBuildIndex." + indexId + "." + partitionId;
    }
}
