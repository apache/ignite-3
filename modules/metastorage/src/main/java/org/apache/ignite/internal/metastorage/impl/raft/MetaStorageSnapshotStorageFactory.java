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

package org.apache.ignite.internal.metastorage.impl.raft;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.local.LocalSnapshotStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot storage factory implementation for Meta Storage.
 */
public class MetaStorageSnapshotStorageFactory implements SnapshotStorageFactory {
    private final KeyValueStorage storage;

    /** Snapshot meta, constructed from the storage data and raft group configuration at startup. {@code null} if the storage is empty. */
    private final @Nullable RaftOutter.SnapshotMeta startupSnapshotMeta;

    private final RaftGroupConfigurationConverter configurationConverter = new RaftGroupConfigurationConverter();

    /**
     * Constructor. We will try to read a snapshot meta here.
     *
     * @param storage Key-value storage instance.
     */
    public MetaStorageSnapshotStorageFactory(KeyValueStorage storage) {
        this.storage = storage;

        startupSnapshotMeta = readStartupSnapshotMeta();
    }

    private @Nullable SnapshotMeta readStartupSnapshotMeta() {
        IndexWithTerm indexWithTerm = storage.getIndexWithTerm();

        if (indexWithTerm == null) {
            return null;
        }

        byte[] configBytes = storage.getConfiguration();
        assert configBytes != null;

        RaftGroupConfiguration configuration = configurationConverter.fromBytes(configBytes);
        assert configuration != null;

        return new RaftMessagesFactory().snapshotMeta()
                .cfgIndex(configuration.index())
                .cfgTerm(configuration.term())
                .lastIncludedIndex(indexWithTerm.index())
                .lastIncludedTerm(indexWithTerm.term())
                .peersList(configuration.peers())
                .oldPeersList(configuration.oldPeers())
                .learnersList(configuration.learners())
                .sequenceToken(configuration.sequenceToken())
                .oldSequenceToken(configuration.oldSequenceToken())
                .oldLearnersList(configuration.oldLearners())
                .build();
    }

    @Override
    public SnapshotStorage createSnapshotStorage(String uri, RaftOptions raftOptions) {
        return new LocalSnapshotStorage(uri, raftOptions) {
            private final AtomicBoolean startupSnapshotOpened = new AtomicBoolean(false);

            @Override
            public SnapshotReader open() {
                if (startupSnapshotOpened.compareAndSet(false, true)) {
                    if (startupSnapshotMeta == null) {
                        // The storage is empty, let's behave how JRaft does: return null, avoiding an attempt to load a snapshot
                        // when it's not there.
                        return null;
                    }

                    return new StartupMetaStorageSnapshotReader(startupSnapshotMeta);
                }

                return super.open();
            }
        };
    }
}
