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

package org.apache.ignite.raft.server.snasphot;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;

/**
 * The factory for a snapshot storage, which is persisted in the map.
 */
public class SnapshotInMemoryStorageFactory implements SnapshotStorageFactory {
    /** The map to store a store metadata. */
    private final Map<String, SnapshotMeta> metaStorage;

    /**
     * The constructor.
     *
     * @param metaStorage Map storage for snapshot metadata.
     */
    public SnapshotInMemoryStorageFactory(Map<String, SnapshotMeta> metaStorage) {
        this.metaStorage = metaStorage;
    }

    @Override
    public SnapshotStorage createSnapshotStorage(String uri, RaftOptions raftOptions) {
        return new SnapshotStorage() {
            @Override
            public boolean setFilterBeforeCopyRemote() {
                return false;
            }

            @Override
            public SnapshotWriter create() {
                return new SnapshotWriter() {
                    @Override
                    public boolean saveMeta(SnapshotMeta meta) {
                        metaStorage.put(uri, meta);
                        return true;
                    }

                    @Override
                    public boolean addFile(String fileName, Message fileMeta) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean removeFile(String fileName) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close(boolean keepDataOnError) throws IOException {
                        // No-op.
                    }

                    @Override
                    public void close() throws IOException {
                        // No-op.
                    }

                    @Override
                    public boolean init(Void opts) {
                        return true;
                    }

                    @Override
                    public void shutdown() {
                        // No-op.
                    }

                    @Override
                    public String getPath() {
                        return uri;
                    }

                    @Override
                    public Set<String> listFiles() {
                        // No files in the snapshot.
                        return Set.of();
                    }

                    @Override
                    public Message getFileMeta(String fileName) {
                        // No files in the snapshot.
                        return null;
                    }
                };
            }

            @Override
            public SnapshotReader open() {
                var snapMeta = metaStorage.get(uri);

                if (snapMeta == null) {
                    return null;
                }

                return new SnapshotReader() {
                    @Override
                    public SnapshotMeta load() {
                        return snapMeta;
                    }

                    @Override
                    public String generateURIForCopy() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() throws IOException {
                        // No-op.
                    }

                    @Override
                    public boolean init(Void opts) {
                        return true;
                    }

                    @Override
                    public void shutdown() {
                        // No-op.
                    }

                    @Override
                    public String getPath() {
                        return uri;
                    }

                    @Override
                    public Set<String> listFiles() {
                        // No files in the snapshot.
                        return Set.of();
                    }

                    @Override
                    public Message getFileMeta(String fileName) {
                        return null;
                    }
                };
            }

            @Override
            public SnapshotReader copyFrom(String uri, SnapshotCopierOptions opts) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SnapshotCopier startToCopyFrom(String uri, SnapshotCopierOptions opts) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {

            }

            @Override
            public boolean init(Void opts) {
                return true;
            }

            @Override
            public void shutdown() {
                // No-op.
            }
        };
    }
}
