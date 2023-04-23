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

package org.apache.ignite.raft.jraft.rpc.impl;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.network.ClusterNode;

/**
* Listener for Raft group events on client side.
*/
public class RaftGroupEventsClientListener {
    private static final IgniteLogger LOG = Loggers.forClass(RaftGroupEventsClientListener.class);

    private final Map<ReplicationGroupId, List<BiConsumer<ClusterNode, Long>>> leaderElectionListeners = new ConcurrentHashMap<>();

    /**
     * Register leader election listener for client.
     *
     * @param groupId Group id.
     * @param listener Listener.
    */
    public void addLeaderElectionListener(ReplicationGroupId groupId, BiConsumer<ClusterNode, Long> listener) {
        leaderElectionListeners.compute(groupId, (k, listeners) -> {
            if (listeners == null) {
                listeners = new ArrayList<>();
            }

            listeners.add(listener);

            return listeners;
        });
    }

        /**
         * Unregister leader election listener for client.
         *
         * @param groupId Group id.
         * @param listener Listener.
        */
    public void removeLeaderElectionListener(ReplicationGroupId groupId, BiConsumer<ClusterNode, Long> listener) {
        leaderElectionListeners.compute(groupId, (k, listeners) -> {
            if (listeners == null) {
                return null;
            }

            listeners.remove(listener);

            return listeners;
        });
    }

    /**
     * Called by RPC processor on leader election notification.
     *
     * @param groupId Group id.
     * @param leader New group leader.
     * @param term Election term.
    */
    public void onLeaderElected(ReplicationGroupId groupId, ClusterNode leader, long term) {
        List<BiConsumer<ClusterNode, Long>> listeners = leaderElectionListeners.get(groupId);

        if (listeners != null) {
            for (BiConsumer<ClusterNode, Long> listener : listeners) {
                try {
                    listener.accept(leader, term);
                } catch (Exception e) {
                    LOG.warn("Failed to notify leader election listener for group=" + groupId, e);
                }
            }
        }
    }
}
