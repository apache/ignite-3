/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import java.util.Set;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;

class PartitionSnapshotWriter extends SnapshotWriter {
    private final PartitionSnapshotStorage partitionSnapshotStorage;

    public PartitionSnapshotWriter(PartitionSnapshotStorage partitionSnapshotStorage) {
        this.partitionSnapshotStorage = partitionSnapshotStorage;
    }

    /** {@inheritDoc} */
    @Override
    public boolean init(Void opts) {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public String getPath() {
        return partitionSnapshotStorage.snapshotUri;
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> listFiles() {
        return Set.of();
    }

    /** {@inheritDoc} */
    @Override
    public Message getFileMeta(String fileName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean saveMeta(SnapshotMeta meta) {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean addFile(String fileName, Message fileMeta) {
        throw new UnsupportedOperationException("addFile");
    }

    /** {@inheritDoc} */
    @Override
    public boolean removeFile(String fileName) {
        throw new UnsupportedOperationException("removeFile");
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
    }

    /** {@inheritDoc} */
    @Override
    public void close(boolean keepDataOnError) {
    }

    /** {@inheritDoc} */
    @Override
    public void shutdown() {
    }
}
