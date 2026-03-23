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

package org.apache.ignite.internal.storage.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;
import org.apache.ignite.table.QualifiedName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests basic functionality of storage engines. Allows for more complex scenarios than {@link AbstractMvTableStorageTest}, because it
 * doesn't limit the usage of the engine with a single table.
 */
@ExtendWith(MockitoExtension.class)
public abstract class AbstractStorageEngineTest extends BaseMvStoragesTest {
    /** Engine instance. */
    protected StorageEngine storageEngine;

    protected LogSyncer logSyncer = mock(LogSyncer.class);

    @BeforeEach
    void createEngineBeforeTest() {
        storageEngine = createEngine();

        storageEngine.start();
    }

    @AfterEach
    void stopEngineAfterTest() {
        if (storageEngine != null) {
            storageEngine.stop();
        }
    }

    /**
     * Creates a new storage engine instance. For persistent engines, the instances within a single test method should point to the same
     * directory.
     */
    protected abstract StorageEngine createEngine();

    @Test
    protected void tableMetrics() {
        var tableDescriptor = new StorageTableDescriptor(10, 1, CatalogService.DEFAULT_STORAGE_PROFILE);
        storageEngine.createMvTable(tableDescriptor, indexId -> null);

        QualifiedName tableName = QualifiedName.of(QualifiedName.DEFAULT_SCHEMA_NAME, "foo");

        StorageEngineTablesMetricSource metricSource = new StorageEngineTablesMetricSource(storageEngine.name(), tableName);
        storageEngine.addTableMetrics(tableDescriptor, metricSource);

        metricSource.enable();
        assertThat(metricSource.holder().metrics(), is(emptyIterable()));
    }
}
