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

package org.apache.ignite.raft.jraft.storage.impl;

import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.RaftMetaStorageOptions;
import org.apache.ignite.raft.jraft.storage.VolatileStorage;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VolatileRaftMetaStorageTest {
    private final VolatileRaftMetaStorage storage = new VolatileRaftMetaStorage();

    private final PeerId peerId = new PeerId("1.2.3.4", 4000);

    @Test
    void initReturnsTrue() {
        assertTrue(storage.init(new RaftMetaStorageOptions()));
    }

    @Test
    void shutdownDoesNothing() {
        assertDoesNotThrow(storage::shutdown);
    }

    @Test
    void returnsDefaultValuesWhenNothingSet() {
        assertThat(storage.getTerm(), is(0L));
        assertThat(storage.getVotedFor(), is(PeerId.emptyPeer()));
    }

    @Test
    void setTermReturnsTrue() {
        assertTrue(storage.setTerm(42));
    }

    @Test
    void setsAndReturnsTerm() {
        storage.setTerm(3);

        assertThat(storage.getTerm(), is(3L));
    }

    @Test
    void setVotedForReturnsTrue() {
        assertTrue(storage.setVotedFor(peerId));
    }

    @Test
    void setsAndReturnsPeer() {
        storage.setVotedFor(peerId);

        assertThat(storage.getVotedFor(), is(sameInstance(peerId)));
    }

    @Test
    void setsTermAndVotedForTogether() {
        storage.setTermAndVotedFor(4, peerId);

        assertThat(storage.getTerm(), is(4L));
        assertThat(storage.getVotedFor(), is(sameInstance(peerId)));
    }

    @Test
    void isInstanceOfVolatileStorage() {
        assertThat(storage, is(instanceOf(VolatileStorage.class)));
    }
}