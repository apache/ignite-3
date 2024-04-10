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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.hlc.TestClockService.TEST_MAX_CLOCK_SKEW_MILLIS;
import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.util.List;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.commands.TablePrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
class CatalogStorageIndexDescriptorSupplierTest extends BaseIgniteAbstractTest {
    private static final long MIN_DATA_AVAILABILITY_TIME = DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS
            + TEST_MAX_CLOCK_SKEW_MILLIS;

    private static final String TABLE_NAME = "TEST";

    private static final String INDEX_NAME = "TEST_IDX";

    private final HybridClock clock = new HybridClockImpl();

    private CatalogManager catalogManager;

    private TestLowWatermark lowWatermark;

    private StorageIndexDescriptorSupplier indexDescriptorSupplier;

    @SuppressWarnings("JUnitMalformedDeclaration")
    @BeforeEach
    void setUp(
            TestInfo testInfo,
            @InjectConfiguration("mock.dataAvailabilityTime = " + MIN_DATA_AVAILABILITY_TIME)
            LowWatermarkConfiguration lowWatermarkConfiguration,
            @Mock VaultManager vaultManager,
            @Mock FailureProcessor failureProcessor
    ) {
        String nodeName = testNodeName(testInfo, 0);

        catalogManager = createTestCatalogManager(nodeName, clock);

        lowWatermark = new TestLowWatermark();

        indexDescriptorSupplier = new CatalogStorageIndexDescriptorSupplier(catalogManager, lowWatermark);

        assertThat(catalogManager.start(), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.stopAll(catalogManager);
    }

    @Test
    void testGetMissingIndex() {
        assertThat(indexDescriptorSupplier.get(0), is(nullValue()));
    }

    @Test
    void testGetLatestAliveIndex() {
        int indexId = createIndex();

        StorageIndexDescriptor indexDescriptor = indexDescriptorSupplier.get(indexId);

        assertThat(indexDescriptor, is(notNullValue()));
        assertThat(indexDescriptor.id(), is(indexId));
    }

    @Test
    void testGetDroppedIndex() {
        int indexId = createIndex();

        CatalogCommand dropIndexCommand = DropIndexCommand.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .indexName(INDEX_NAME)
                .build();

        assertThat(catalogManager.execute(dropIndexCommand), willCompleteSuccessfully());

        assertThat(catalogManager.index(indexId, catalogManager.latestCatalogVersion()), is(nullValue()));

        StorageIndexDescriptor indexDescriptor = indexDescriptorSupplier.get(indexId);

        assertThat(indexDescriptor, is(notNullValue()));
        assertThat(indexDescriptor.id(), is(indexId));
    }

    @Test
    void testGetAliveIndexAfterWatermark() throws InterruptedException {
        int indexId = createIndex();

        HybridTimestamp timestampAfterCreate = clock.now();

        StorageIndexDescriptor indexDescriptor = indexDescriptorSupplier.get(indexId);

        assertThat(indexDescriptor, is(notNullValue()));
        assertThat(indexDescriptor.id(), is(indexId));

        raiseWatermarkUpTo(timestampAfterCreate);

        indexDescriptor = indexDescriptorSupplier.get(indexId);

        assertThat(indexDescriptor, is(notNullValue()));
        assertThat(indexDescriptor.id(), is(indexId));
    }

    @Test
    void testGetDroppedIndexAfterWatermark() throws InterruptedException {
        int indexId = createIndex();

        CatalogCommand dropIndexCommand = DropIndexCommand.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .indexName(INDEX_NAME)
                .build();

        assertThat(catalogManager.execute(dropIndexCommand), willCompleteSuccessfully());

        HybridTimestamp timestampAfterDrop = clock.now();

        StorageIndexDescriptor indexDescriptor = indexDescriptorSupplier.get(indexId);

        assertThat(indexDescriptor, is(notNullValue()));
        assertThat(indexDescriptor.id(), is(indexId));

        raiseWatermarkUpTo(timestampAfterDrop);

        indexDescriptor = indexDescriptorSupplier.get(indexId);

        assertThat(indexDescriptor, is(nullValue()));
    }

    private int createIndex() {
        TablePrimaryKey primaryKey = TableHashPrimaryKey.builder()
                .columns(List.of("foo"))
                .build();

        List<CatalogCommand> commands = List.of(
                CreateTableCommand.builder()
                        .schemaName(DEFAULT_SCHEMA_NAME)
                        .tableName(TABLE_NAME)
                        .columns(List.of(ColumnParams.builder().name("foo").type(ColumnType.INT32).build()))
                        .primaryKey(primaryKey)
                        .build(),
                CreateHashIndexCommand.builder()
                        .schemaName(DEFAULT_SCHEMA_NAME)
                        .tableName(TABLE_NAME)
                        .indexName(INDEX_NAME)
                        .columns(List.of("foo"))
                        .build()
        );

        assertThat(catalogManager.execute(commands), willCompleteSuccessfully());

        CatalogIndexDescriptor index = catalogManager.aliveIndex(INDEX_NAME, clock.nowLong());

        assertThat(index, is(notNullValue()));

        return index.id();
    }

    private void raiseWatermarkUpTo(HybridTimestamp now) throws InterruptedException {
        assertThat(lowWatermark.updateAndNotify(now), willCompleteSuccessfully());

        HybridTimestamp lowWatermarkTimestamp = lowWatermark.getLowWatermark();

        assertThat(lowWatermarkTimestamp, is(notNullValue()));
        assertThat(catalogManager.activeCatalogVersion(lowWatermarkTimestamp.longValue()), is(catalogManager.latestCatalogVersion()));
    }
}
