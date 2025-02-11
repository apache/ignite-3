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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.startup;

import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.storage.snapshot.startup.StartupSnapshotReader;

/**
 * Snapshot reader used for raft group bootstrap. Reads initial state of the storage.
 */
public class StartupPartitionSnapshotReader extends StartupSnapshotReader {
    /** Instance of snapshot storage for shared fields access. */
    private final PartitionSnapshotStorage snapshotStorage;

    /**
     * Constructor.
     *
     * @param snapshotStorage Snapshot storage.
     */
    public StartupPartitionSnapshotReader(PartitionSnapshotStorage snapshotStorage) {
        this.snapshotStorage = snapshotStorage;
    }

    /** {@inheritDoc} */
    @Override
    public SnapshotMeta load() {
        return snapshotStorage.startupSnapshotMeta();
    }

    /** {@inheritDoc} */
    @Override
    public String getPath() {
        return snapshotStorage.snapshotUri();
    }
}
