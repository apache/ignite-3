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

package org.apache.ignite.internal.raft;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PeersAndLearners} class.
 */
class PeersAndLearnersTest {
    @Test
    void testFromPeers() {
        var peers = Set.of(new Peer("foo"), new Peer("bar"));
        var learners = Set.of(new Peer("baz"));

        PeersAndLearners peersAndLearners = PeersAndLearners.fromPeers(peers, learners);

        assertThat(peersAndLearners.peers(), is(peers));
        assertThat(peersAndLearners.learners(), is(learners));
    }

    @Test
    void testFromConsistentIds() {
        var peers = Set.of("foo", "bar");
        var learners = Set.of("baz", "foo");

        PeersAndLearners peersAndLearners = PeersAndLearners.fromConsistentIds(peers, learners);

        assertThat(peersAndLearners.peers(), containsInAnyOrder(new Peer("foo", 0), new Peer("bar", 0)));
        assertThat(peersAndLearners.learners(), containsInAnyOrder(new Peer("foo", 1), new Peer("baz", 0)));
    }
}
