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

import java.nio.file.Path;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.AbstractMvPartitionStorageTest;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Storage test implementation for {@link RocksDbMvPartitionStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class RocksDbMvPartitionStorageTest extends AbstractMvPartitionStorageTest {
    @InjectConfiguration("mock {flushDelayMillis = 0, defaultRegion {size = 16777216, writeBufferSize = 16777216}}")
    private RocksDbStorageEngineConfiguration engineConfig;

    @WorkDirectory
    private Path workDir;

    @Override
    protected StorageEngine createEngine() {
        return new RocksDbStorageEngine(engineConfig, workDir);
    }

    @Override
    public void addWriteCommittedThrowsIfUncommittedVersionExists() {
        // Disable this test because RocksDbMvPartitionStorage does not throw. It does not throw because this
        // exception is thrown only to ease debugging as the caller must make sure that no write intent exists
        // before calling addWriteCommitted(). For RocksDbMvPartitionStorage, it is not that cheap to check whether
        // there is a write intent in the storage, so we do not require it to throw this optional exception.
    }
}
