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

package org.apache.ignite.internal.raft.storage.impl;

import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.RaftMetaStorageOptions;
import org.apache.ignite.raft.jraft.storage.RaftMetaStorage;
import org.apache.ignite.raft.jraft.storage.VolatileStorage;

/**
 * Volatile (in-memory) implementation of {@link RaftMetaStorage}. Used for Raft groups storing partition data of
 * volatile storages.
 */
public class VolatileRaftMetaStorage implements RaftMetaStorage, VolatileStorage {
    private volatile long term;

    private volatile PeerId votedFor = PeerId.emptyPeer();

    @Override
    public boolean init(RaftMetaStorageOptions opts) {
        return true;
    }

    @Override
    public void shutdown() {
        // no-op
    }

    @Override
    public boolean setTerm(long term) {
        this.term = term;

        return true;
    }

    @Override
    public long getTerm() {
        return term;
    }

    @Override
    public boolean setVotedFor(PeerId peerId) {
        this.votedFor = peerId;

        return true;
    }

    @Override
    public PeerId getVotedFor() {
        return votedFor;
    }

    @Override
    public boolean setTermAndVotedFor(long term, PeerId peerId) {
        this.term = term;
        this.votedFor = peerId;

        return true;
    }

    @Override
    public void createAfterDestroy() {
        // no-op
    }
}
