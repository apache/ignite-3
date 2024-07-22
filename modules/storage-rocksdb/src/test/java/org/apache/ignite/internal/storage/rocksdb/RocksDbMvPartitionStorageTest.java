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

package org.apache.ignite.internal.storage.rocksdb;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.AbstractMvPartitionStorageTest;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Storage test implementation for {@link RocksDbMvPartitionStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class RocksDbMvPartitionStorageTest extends AbstractMvPartitionStorageTest {
    private RocksDbStorageEngine engine;

    private RocksDbTableStorage table;

    @BeforeEach
    void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration("mock.flushDelayMillis = 0")
            RocksDbStorageEngineConfiguration engineConfig,
            @InjectConfiguration("mock.profiles.default = {engine = rocksdb, size = 16777216, writeBufferSize = 16777216}")
            StorageConfiguration storageConfiguration
    ) {
        engine = new RocksDbStorageEngine("test", engineConfig, storageConfiguration, workDir, mock(LogSyncer.class));

        engine.start();

        table = engine.createMvTable(
                new StorageTableDescriptor(1, DEFAULT_PARTITION_COUNT, "default"),
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
    public void addWriteCommittedThrowsIfUncommittedVersionExists() {
        // Disable this test because RocksDbMvPartitionStorage does not throw. It does not throw because this
        // exception is thrown only to ease debugging as the caller must make sure that no write intent exists
        // before calling addWriteCommitted(). For RocksDbMvPartitionStorage, it is not that cheap to check whether
        // there is a write intent in the storage, so we do not require it to throw this optional exception.
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22617")
    @Override
    public void estimatedSizeUsingWriteIntents() {
        super.estimatedSizeUsingWriteIntents();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22617")
    @Override
    public void estimatedSizeUsingCommittedWrites() {
        super.estimatedSizeUsingCommittedWrites();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22617")
    @Override
    public void estimatedSizeNeverFallsBelowZeroUsingWriteCommitted() {
        super.estimatedSizeNeverFallsBelowZeroUsingWriteCommitted();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22617")
    @Override
    public void estimatedSizeNeverFallsBelowZeroUsingCommitWrite() {
        super.estimatedSizeNeverFallsBelowZeroUsingCommitWrite();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22617")
    @Override
    public void estimatedSizeShowsLatestRowsNumberUsingWriteCommited() {
        super.estimatedSizeShowsLatestRowsNumberUsingWriteCommited();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22617")
    @Override
    public void estimatedSizeShowsLatestRowsNumberUsingCommitWrite() {
        super.estimatedSizeShowsLatestRowsNumberUsingCommitWrite();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22617")
    @Override
    public void estimatedSizeIsNotAffectedByGarbageTombstones() {
        super.estimatedSizeIsNotAffectedByGarbageTombstones();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22617")
    @Override
    public void estimatedSizeHandlesTransactionAborts() {
        super.estimatedSizeHandlesTransactionAborts();
    }
}
