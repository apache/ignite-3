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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryTableStorage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VolatilePageMemoryMvPartitionStorageTest extends AbstractPageMemoryMvPartitionStorageTest {
    @InjectConfiguration("mock.profiles.default = {engine = aimem}")
    private StorageConfiguration storageConfig;

    private VolatilePageMemoryStorageEngine engine;

    private VolatilePageMemoryTableStorage table;

    @BeforeEach
    void setUp(@InjectConfiguration SystemLocalConfiguration systemConfig) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new VolatilePageMemoryStorageEngine("node", storageConfig, systemConfig, ioRegistry, mock(FailureProcessor.class), clock);

        engine.start();

        table = engine.createMvTable(
                new StorageTableDescriptor(1, DEFAULT_PARTITION_COUNT, DEFAULT_STORAGE_PROFILE),
                mock(StorageIndexDescriptorSupplier.class)
        );

        initialize(table);
    }

    @AfterEach
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        IgniteUtils.closeAllManually(
                table,
                engine == null ? null : engine::stop
        );
    }

    @Override
    int pageSize() {
        return engine.configuration().pageSizeBytes().value();
    }

    @Test
    void addWriteCreatesPlainRowVersion() {
        addWrite(ROW_ID, binaryRow, txId);

        RowVersion rowVersion = pageMemoryStorage().findVersionChain(
                ROW_ID,
                chain -> pageMemoryStorage().readRowVersion(chain.headLink(), ts -> false)
        );
        assertThat(rowVersion, is(notNullValue()));

        assertThat(rowVersion.getClass(), is(RowVersion.class));
    }
}
