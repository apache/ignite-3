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

package org.apache.ignite.internal.storage.pagememory.engine;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;
import static org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfigurationSchema.DEFAULT_PAGE_SIZE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileView;
import org.apache.ignite.internal.storage.engine.AbstractPersistentStorageEngineTest;
import org.apache.ignite.internal.storage.engine.AbstractStorageEngineTest;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource.Holder;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileView;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.table.QualifiedName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Implementation of the {@link AbstractStorageEngineTest} for the {@link PersistentPageMemoryStorageEngine#ENGINE_NAME} engine.
 */
@ExtendWith({WorkDirectoryExtension.class, ExecutorServiceExtension.class})
public class PersistentPageMemoryStorageEngineTest extends AbstractPersistentStorageEngineTest {
    @InjectConfiguration("mock.profiles.default.engine = aipersist")
    private StorageConfiguration storageConfig;

    @InjectConfiguration
    private SystemLocalConfiguration systemConfig;

    @InjectExecutorService
    ExecutorService executorService;

    @WorkDirectory
    private Path workDir;

    @Override
    protected StorageEngine createEngine() {
        return createEngine(storageConfig);
    }

    private StorageEngine createEngine(StorageConfiguration configuration) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PersistentPageMemoryStorageEngine(
                "test",
                mock(MetricManager.class),
                configuration,
                systemConfig,
                ioRegistry,
                workDir,
                null,
                mock(FailureManager.class),
                logSyncer,
                executorService,
                clock
        );
    }

    @Test
    void dataRegionSizeUsedWhenSet(
            @InjectConfiguration("mock.profiles.default {engine = aipersist, sizeBytes = 12345}")
            StorageConfiguration storageConfig
    ) {
        StorageEngine anotherEngine = createEngine(storageConfig);

        anotherEngine.start();

        for (StorageProfileView view : storageConfig.profiles().value()) {
            assertThat(((PersistentPageMemoryProfileView) view).sizeBytes(), is(12345L));
        }

        assertThat(anotherEngine.requiredOffHeapMemorySize(), is(12345L));
    }

    @Override
    protected void persistTableDestructionIfNeeded() {
        // No-op as table destruction is durable for this engine.
    }

    @Test
    @Override
    protected void tableMetrics() {
        var engine = (PersistentPageMemoryStorageEngine) storageEngine;

        var tableDescriptor = new StorageTableDescriptor(10, 1, CatalogService.DEFAULT_STORAGE_PROFILE);
        MvTableStorage table = engine.createMvTable(tableDescriptor, indexId -> null);

        QualifiedName tableName = QualifiedName.of(QualifiedName.DEFAULT_SCHEMA_NAME, "foo");

        StorageEngineTablesMetricSource metricSource = new StorageEngineTablesMetricSource(storageEngine.name(), tableName);
        storageEngine.addTableMetrics(tableDescriptor, metricSource);

        metricSource.enable();

        Holder holder = metricSource.holder();

        assertThat(holder, is(notNullValue()));

        Map<String, Metric> metricByName = stream(holder.metrics().spliterator(), false)
                .collect(toMap(Metric::name, Function.identity()));

        LongMetric totalAllocatedSize = (LongMetric) metricByName.get("TotalAllocatedSize");
        LongMetric totalUsedSize = (LongMetric) metricByName.get("TotalUsedSize");
        LongMetric totalEmptySize = (LongMetric) metricByName.get("TotalEmptySize");
        LongMetric totalDataSize = (LongMetric) metricByName.get("TotalDataSize");
        DoubleMetric pagesFillFactor = (DoubleMetric) metricByName.get("PagesFillFactor");

        assertThat(totalAllocatedSize.value(), is(0L));
        assertThat(totalUsedSize.value(), is(0L));
        assertThat(totalEmptySize.value(), is(0L));
        assertThat(totalDataSize.value(), is(0L));
        assertThat(pagesFillFactor.value(), is(0.0));

        var otherTableDescriptor = new StorageTableDescriptor(20, 1, CatalogService.DEFAULT_STORAGE_PROFILE);
        MvTableStorage otherTable = engine.createMvTable(otherTableDescriptor, indexId -> null);

        assertThat(otherTable.createMvPartition(0), willCompleteSuccessfully());

        assertThat(totalAllocatedSize.value(), is(0L));
        assertThat(totalUsedSize.value(), is(0L));
        assertThat(totalEmptySize.value(), is(0L));
        assertThat(totalDataSize.value(), is(0L));
        assertThat(pagesFillFactor.value(), is(0.0));

        assertThat(table.createMvPartition(0), willCompleteSuccessfully());

        assertThat(totalAllocatedSize.value(), is(greaterThan(0L)));
        assertThat(totalAllocatedSize.value() % DEFAULT_PAGE_SIZE, is(0L));
        assertThat(totalUsedSize.value(), is(totalAllocatedSize.value()));
        assertThat(totalEmptySize.value(), is(0L));
        assertThat(totalDataSize.value(), is(totalAllocatedSize.value()));
        assertThat(pagesFillFactor.value(), is(1.0));

        long totalAllocatedSizeBeforeData = totalAllocatedSize.value();

        MvPartitionStorage partitionStorage = table.getMvPartition(0);

        assertThat(partitionStorage, is(notNullValue()));

        partitionStorage.runConsistently(locker -> {
            RowId rowId = RowId.lowestRowId(0);

            locker.lock(rowId);

            var row = binaryRow(new TestKey(0, "foo"), new TestValue(1, "bar"));

            partitionStorage.addWriteCommitted(rowId, row, clock.now());

            return null;
        });

        assertThat(totalAllocatedSize.value(), is(greaterThan(totalAllocatedSizeBeforeData)));
        assertThat(totalAllocatedSize.value() % DEFAULT_PAGE_SIZE, is(0L));
        assertThat(totalUsedSize.value(), is(totalAllocatedSize.value()));
        assertThat(totalEmptySize.value(), is(0L));
        assertThat(totalDataSize.value(), is(lessThan(totalAllocatedSize.value())));
        assertThat(pagesFillFactor.value(), is(allOf(greaterThan(0.0), lessThan(1.0))));
    }
}
