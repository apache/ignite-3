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

package org.apache.ignite.internal.table.distributed.raft.snapshot.incoming;

import java.util.Set;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;

/**
 * Snapshot reader implementation to read the metadata of downloaded snapshot.
 */
class IncomingSnapshotReader extends SnapshotReader {
    /** Snapshot meta. */
    private final SnapshotMeta snapshotMeta;

    /**
     * Constructor.
     *
     * @param snapshotMeta Snapshot meta.
     */
    IncomingSnapshotReader(SnapshotMeta snapshotMeta) {
        this.snapshotMeta = snapshotMeta;
    }

    @Override
    public SnapshotMeta load() {
        return snapshotMeta;
    }

    @Override
    public boolean init(Void opts) {
        // No-op.
        return true;
    }

    @Override
    public String generateURIForCopy() {
        throw new UnsupportedOperationException("This snapshot cannot be used as a source for copies.");
    }

    @Override
    public void close() {
        // No-op.
    }

    @Override
    public void shutdown() {
        // No-op.
    }

    @Override
    public String getPath() {
        // We must not throw anything from here to not impede the snapshot installation process. PartitionListener#onSnapshotLoad
        // ignores the path passed to it, so it doesn't matter what we return here, hence an empty string is fine.
        return "";
    }

    @Override
    public Set<String> listFiles() {
        // No files in the snapshot.
        return Set.of();
    }

    @Override
    public Message getFileMeta(String fileName) {
        throw new UnsupportedOperationException("No files in the snapshot");
    }
}
