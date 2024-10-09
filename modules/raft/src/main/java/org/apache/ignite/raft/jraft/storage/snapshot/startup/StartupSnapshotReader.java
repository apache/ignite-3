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

package org.apache.ignite.raft.jraft.storage.snapshot.startup;

import java.io.IOException;
import java.util.Set;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;

/**
 * Snapshot reader used for raft group bootstrap. Reads initial state of the storage.
 */
public abstract class StartupSnapshotReader extends SnapshotReader {
    @Override
    public Set<String> listFiles() {
        return Set.of();
    }

    @Override
    public Message getFileMeta(String fileName) {
        throw new UnsupportedOperationException("No files in the snapshot");
    }

    @Override
    public String generateURIForCopy() {
        throw new UnsupportedOperationException("Can't copy a startup snapshot");
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
    public void close() throws IOException {
        // No-op.
    }
}
