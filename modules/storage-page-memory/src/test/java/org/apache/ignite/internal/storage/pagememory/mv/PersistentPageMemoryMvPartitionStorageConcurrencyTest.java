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

import java.nio.file.Path;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.AbstractMvPartitionStorageConcurrencyTest;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class PersistentPageMemoryMvPartitionStorageConcurrencyTest extends AbstractMvPartitionStorageConcurrencyTest {
    @InjectConfiguration("mock.checkpoint.checkpointDelayMillis = 0")
    private PersistentPageMemoryStorageEngineConfiguration engineConfig;

    @WorkDirectory
    private Path workDir;

    @Override
    protected StorageEngine createEngine() {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PersistentPageMemoryStorageEngine("test", engineConfig, ioRegistry, workDir, null);
    }

    @Disabled("IGNITE-18023")
    @Override
    public void testTombstoneGcAndAddWrite(AddAndCommit addAndCommit) {
        super.testTombstoneGcAndAddWrite(addAndCommit);
    }

    @Disabled("IGNITE-18023")
    @Override
    public void testTombstoneGcAndRead(AddAndCommit addAndCommit) {
        super.testTombstoneGcAndRead(addAndCommit);
    }

    @Disabled("IGNITE-18023")
    @Override
    public void testConcurrentGc(AddAndCommit addAndCommit) {
        super.testConcurrentGc(addAndCommit);
    }

    @Disabled("IGNITE-18023")
    @Override
    public void testTombstoneGcAndCommitWrite(AddAndCommit addAndCommit) {
        super.testTombstoneGcAndCommitWrite(addAndCommit);
    }

    @Disabled("IGNITE-18023")
    @Override
    public void testTombstoneGcAndAbortWrite(AddAndCommit addAndCommit) {
        super.testTombstoneGcAndAbortWrite(addAndCommit);
    }

    @Disabled("IGNITE-18023")
    @Override
    public void testRegularGcAndRead(AddAndCommit addAndCommit) {
        super.testRegularGcAndRead(addAndCommit);
    }
}
