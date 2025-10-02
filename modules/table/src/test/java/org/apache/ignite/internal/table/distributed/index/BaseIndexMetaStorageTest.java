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

package org.apache.ignite.internal.table.distributed.index;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.addColumnToTable;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableStrict;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/** Base class for testing {@link IndexMetaStorage}. */
abstract class BaseIndexMetaStorageTest extends BaseIgniteAbstractTest {
    static final int DELTA_TO_TRIGGER_DESTROY = 1_000_000;

    static final String NODE_NAME = "test";

    static final String NEW_TABLE_NAME = TABLE_NAME + "_NEW";

    static final String NEW_PK_INDEX_NAME = pkIndexName(NEW_TABLE_NAME);

    final HybridClock clock = new HybridClockImpl();

    private final TestLowWatermark lowWatermark = new TestLowWatermark();

    MetaStorageManager metastore;

    CatalogManager catalogManager;

    IndexMetaStorage indexMetaStorage;

    abstract MetaStorageManager createMetastore();

    abstract CatalogManager createCatalogManager();

    @BeforeEach
    void setUp() {
        createComponents();

        var componentContext = new ComponentContext();

        assertThat(startAsync(componentContext, metastore), willCompleteSuccessfully());
        assertThat(metastore.recoveryFinishedFuture(), willCompleteSuccessfully());

        assertThat(startAsync(componentContext, catalogManager, indexMetaStorage), willCompleteSuccessfully());

        assertThat(metastore.deployWatches(), willCompleteSuccessfully());

        assertThat(catalogManager.catalogInitializationFuture(), willCompleteSuccessfully());

        createSimpleTable(catalogManager, TABLE_NAME);
    }

    @AfterEach
    void tearDown() throws Exception {
        var componentContext = new ComponentContext();

        IgniteUtils.closeAll(
                indexMetaStorage == null ? null : indexMetaStorage::beforeNodeStop,
                catalogManager == null ? null : catalogManager::beforeNodeStop,
                metastore == null ? null : metastore::beforeNodeStop,
                () -> assertThat(
                        stopAsync(componentContext, indexMetaStorage, catalogManager, metastore),
                        willCompleteSuccessfully()
                )
        );
    }

    void createComponents() {
        metastore = createMetastore();

        catalogManager = createCatalogManager();

        indexMetaStorage = new IndexMetaStorage(catalogManager, lowWatermark, metastore);
    }

    int indexId(String indexName) {
        return getIndexIdStrict(catalogManager, indexName, clock.nowLong());
    }

    int tableId(String tableName) {
        return getTableIdStrict(catalogManager, tableName, clock.nowLong());
    }

    @Nullable IndexMeta fromMetastore(int indexId) {
        byte[] versionBytes = getFromMetastore(ByteArray.fromString("index.meta.version." + indexId)).value();
        byte[] valueBytes = getFromMetastore(ByteArray.fromString("index.meta.value." + indexId)).value();

        if (valueBytes == null) {
            assertNull(versionBytes, "indexId=" + indexId);

            return null;
        }

        assertNotNull(versionBytes, "indexId=" + indexId);

        int catalogVersion = ByteUtils.bytesToIntKeepingOrder(versionBytes);
        IndexMeta indexMeta = VersionedSerialization.fromBytes(valueBytes, IndexMetaSerializer.INSTANCE);

        assertEquals(indexMeta.catalogVersion(), catalogVersion, "indexId=" + indexId);

        return indexMeta;
    }

    MetaIndexStatusChange toChangeInfo(int catalogVersion) {
        Catalog catalog = catalogManager.catalog(catalogVersion);

        assertNotNull(catalog, "catalogVersion=" + catalogVersion);

        return new MetaIndexStatusChange(catalog.version(), catalog.time());
    }

    List<String> allIndexNamesFromSnapshotIndexMetas() {
        return indexMetaStorage.indexMetas().stream()
                .map(IndexMeta::indexName)
                .collect(toList());
    }

    int executeCatalogUpdate(Runnable task) {
        task.run();

        return latestCatalogVersion();
    }

    void updateLwm(HybridTimestamp newLwm) {
        assertThat(lowWatermark.updateAndNotify(newLwm), willCompleteSuccessfully());
    }

    static void checkFields(
            @Nullable IndexMeta indexMeta,
            int expIndexId,
            int expTableId,
            int expTableVersion,
            String expIndexName,
            MetaIndexStatus expStatus,
            Map<MetaIndexStatus, MetaIndexStatusChange> expStatuses,
            int expCatalogVersion
    ) {
        assertNotNull(indexMeta, "indexId=" + expIndexId);

        assertEquals(expIndexId, indexMeta.indexId());
        assertEquals(expTableId, indexMeta.tableId(), "indexId=" + expIndexId);
        assertEquals(expTableVersion, indexMeta.tableVersion(), "indexId=" + expIndexId);
        assertEquals(expIndexName, indexMeta.indexName(), "indexId=" + expIndexId);
        assertEquals(expStatus, indexMeta.status(), "indexId=" + expIndexId);
        assertEquals(expStatuses, indexMeta.statusChanges(), "indexId=" + expIndexId);
        assertEquals(expCatalogVersion, indexMeta.catalogVersion(), "indexId=" + expIndexId);
    }

    private Entry getFromMetastore(ByteArray key) {
        CompletableFuture<Entry> future = metastore.get(key);

        assertThat(future, willCompleteSuccessfully());

        return future.join();
    }

    int latestCatalogVersion() {
        return catalogManager.latestCatalogVersion();
    }

    int latestTableVersionFromCatalog(int tableId) {
        return getTableStrict(catalogManager, tableId, clock.nowLong()).latestSchemaVersion();
    }

    void updateTableVersion(String tableName) {
        int tableId = tableId(tableName);

        int tableVersionBeforeUpdate = latestTableVersionFromCatalog(tableId);

        addColumnToTable(catalogManager, DEFAULT_SCHEMA_NAME, tableName, tableName + "_NEW_COLUMN", INT32);

        int tableVersionAfterUpdate = latestTableVersionFromCatalog(tableId);

        assertThat(tableVersionAfterUpdate, greaterThan(tableVersionBeforeUpdate));
    }
}
