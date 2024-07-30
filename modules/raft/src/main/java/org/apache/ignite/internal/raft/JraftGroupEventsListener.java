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

import java.util.Collection;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;

/**
 * Listener for group membership and other events.
 */
public interface JraftGroupEventsListener {
    /**
     * Invoked, when a new leader is elected (if it is the first leader of group ever - will be invoked too).
     *
     * @param term Raft term of the current leader.
     */
    void onLeaderElected(long term);

    /**
     * Invoked on the leader, when new peers' configuration applied to raft group.
     *
     * @param peers Collection of peers, which was applied by raft group membership configuration.
     * @param learners Collection of learners, which was applied by raft group membership configuration.
     * @param term Raft term of the current leader.
     */
    void onNewPeersConfigurationApplied(Collection<PeerId> peers, Collection<PeerId> learners, long term);

    /**
     * Invoked on the leader if membership reconfiguration failed, because of {@link Status}.
     *
     * @param status Description of failure.
     * @param peers Collection of peers, which was as a target of reconfiguration.
     * @param learners Collection of learners, which was as a target of reconfiguration.
     * @param term Raft term of the current leader.
     */
    void onReconfigurationError(Status status, Collection<PeerId> peers, Collection<PeerId> learners, long term);
}
