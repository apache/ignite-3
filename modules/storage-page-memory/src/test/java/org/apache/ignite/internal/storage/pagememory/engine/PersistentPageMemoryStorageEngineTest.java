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

import static org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfigurationSchema.DEFAULT_PAGE_SIZE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileView;
import org.apache.ignite.internal.storage.engine.AbstractPersistentStorageEngineTest;
import org.apache.ignite.internal.storage.engine.AbstractStorageEngineTest;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;
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
                null,
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
        Iterator<Metric> metrics = metricSource.holder().metrics().iterator();
        assertTrue(metrics.hasNext());

        Metric metric = metrics.next();
        assertThat(metric.name(), is("TotalAllocatedSize"));
        assertThat(metric, is(instanceOf(LongMetric.class)));

        assertFalse(metrics.hasNext());

        LongMetric totalAllocatedSize = (LongMetric) metric;
        assertEquals(0, totalAllocatedSize.value());

        var otherTableDescriptor = new StorageTableDescriptor(20, 1, CatalogService.DEFAULT_STORAGE_PROFILE);
        MvTableStorage otherTable = engine.createMvTable(otherTableDescriptor, indexId -> null);

        otherTable.createMvPartition(0);
        assertEquals(0, totalAllocatedSize.value());

        table.createMvPartition(0);
        assertThat(totalAllocatedSize.value(), is(greaterThan(0L)));
        assertEquals(0, totalAllocatedSize.value() % DEFAULT_PAGE_SIZE);
    }
}
