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

package org.apache.ignite.internal.metastorage.server;

import java.nio.file.Path;
import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.jetbrains.annotations.TestOnly;

/**
 * Test version of {@link RocksDbKeyValueStorage}, but behavior on a start differs. In this version, storage is not destroyed on
 * a restart, so it can be used in {@link org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager}, where raft
 * layer is mocked and there are no raft's snapshot installing and log playback.
 */
@TestOnly
public class TestRocksDbKeyValueStorage extends RocksDbKeyValueStorage {
    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param dbPath RocksDB path.
     */
    public TestRocksDbKeyValueStorage(String nodeName, Path dbPath) {
        super(nodeName, dbPath, new NoOpFailureProcessor(nodeName));
    }

    @Override
    protected void destroyRocksDb() {
        // We don't want to remove all data after restart, because we cannot rely on a raft's snapshot installing and log playback.
    }
}
