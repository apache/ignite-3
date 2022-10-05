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

package org.apache.ignite.internal.table.distributed.raft.snapshot.startup;

import java.io.IOException;
import java.util.Set;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;

/**
 * Snapshot reader used for raft group bootstrap. Reads initial state of the storage.
 */
public class StartupPartitionSnapshotReader extends SnapshotReader {
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

    /** {@inheritDoc} */
    @Override
    public Set<String> listFiles() {
        // No files in the snapshot.
        return Set.of();
    }

    /** {@inheritDoc} */
    @Override
    public Message getFileMeta(String fileName) {
        throw new UnsupportedOperationException("No files in the snapshot");
    }

    /** {@inheritDoc} */
    @Override
    public String generateURIForCopy() {
        throw new UnsupportedOperationException("Can't copy a startup snapshot");
    }

    /** {@inheritDoc} */
    @Override
    public boolean init(Void opts) {
        // No-op.
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void shutdown() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        // No-op.
    }
}
