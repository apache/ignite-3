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

package org.apache.ignite.internal.datareplication.snapshot;

import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;

/**
 * Temporary snapshot factory, which must help to identify the unexpected snapshots,
 * while zone based replicas is not support snapshotting yet.
 */
// TODO https://issues.apache.org/jira/browse/IGNITE-22115 remove it
public class FailFastSnapshotStorageFactory implements SnapshotStorageFactory {
    @Override
    public SnapshotStorage createSnapshotStorage(String uri, RaftOptions raftOptions) {
        return new SnapshotStorage() {

            private <T> T fail() {
                throw new UnsupportedOperationException("Snapshotting is not implemented yet for the zone based partitions");
            }

            @Override
            public boolean setFilterBeforeCopyRemote() {
                return fail();
            }

            @Override
            public SnapshotWriter create() {
                return fail();
            }

            @Override
            public SnapshotReader open() {
                return null;
            }

            @Override
            public SnapshotReader copyFrom(String uri, SnapshotCopierOptions opts) {
                return fail();
            }

            @Override
            public SnapshotCopier startToCopyFrom(String uri, SnapshotCopierOptions opts) {
                return fail();
            }

            @Override
            public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
                // No-op
            }

            @Override
            public boolean init(Void opts) {
                return true;
            }

            @Override
            public void shutdown() {
                // No-op
            }
        };
    }
}
