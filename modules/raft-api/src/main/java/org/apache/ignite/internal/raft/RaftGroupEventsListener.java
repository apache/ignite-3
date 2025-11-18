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

/**
 * Listener for group membership and other events.
 */
public interface RaftGroupEventsListener {
    /**
     * Invoked, when this node is elected as a new leader (if it is the first leader of group ever - will be invoked too).
     *
     * @param term Raft term of the current leader.
     * @param configurationTerm Term on which the current configuration was applied.
     * @param configurationIndex Index on which the current configuration was applied.
     * @param configuration Raft configuration on the moment of leader election.
     * @param sequenceToken Sequence token of this change.
     */
    void onLeaderElected(
            long term,
            long configurationTerm,
            long configurationIndex,
            PeersAndLearners configuration,
            long sequenceToken
    );

    /**
     * Invoked on the leader, when new peers' configuration applied to raft group.
     *
     * @param configuration New Raft group configuration.
     * @param term Term on which the new configuration was applied.
     * @param index Index on which the new configuration was applied.
     */
    default void onNewPeersConfigurationApplied(PeersAndLearners configuration, long term, long index) {}

    /**
     * Invoked on the leader if membership reconfiguration failed, because of {@link Status}.
     *
     * @param status Description of failure.
     * @param configuration Configuration that failed to be applied.
     * @param term Raft term of the current leader.
     * @param sequenceToken Sequence token of the current change.
     */
    default void onReconfigurationError(Status status, PeersAndLearners configuration, long term, long sequenceToken) {}

    /**
     * No-op raft group events listener.
     */
    RaftGroupEventsListener noopLsnr = (term, configurationTerm, configurationIndex, configuration, sequenceToken) -> {};
}
