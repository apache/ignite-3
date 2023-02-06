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

package org.apache.ignite.internal.raft.server.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.network.ClusterNode;

/**
 * RAFT server event listener.
 */
public class RaftServiceEventListener {
    /** Callbacks by group ids. */
    private ConcurrentHashMap<ReplicationGroupId, Set<Consumer<Long>>> subscriptions = new ConcurrentHashMap<>();

    /** Callbacks by cluster nodes. */
    private HashMap<ClusterNode, Set<Consumer<Long>>> nosesSubscriptions = new HashMap<>();


    /**
     * Subscribes a node to notification.
     *
     * @param groupId Group id.
     * @param subscriber Subscriber node.
     * @param notifyAction Notify action.
     */
   public void subscribe(ReplicationGroupId groupId, ClusterNode subscriber, Consumer<Long> notifyAction) {
       subscriptions.compute(groupId, (id, actions) -> {
           if (actions == null) {
               actions = new HashSet<>();
           }

           actions.add(notifyAction);
           nosesSubscriptions.computeIfAbsent(subscriber, node -> new HashSet<>())
                   .add(notifyAction);

           return actions;
       });
   }

    /**
     * Unsubscribes a node.
     *
     * @param groupId Group id.
     * @param clusterNode Subscriber node.
     */
   public void unsubscribe(ReplicationGroupId groupId, ClusterNode clusterNode) {
       subscriptions.compute(groupId, (id, actions) -> {
           if (CollectionUtils.nullOrEmpty(actions)) {
               return null;
           }

           Set<Consumer<Long>> nodeActions = nosesSubscriptions.get(clusterNode);

           assert !CollectionUtils.nullOrEmpty(nodeActions);

           Set<Consumer<Long>> grpNodeActions = new HashSet<>(actions);

           grpNodeActions.retainAll(nodeActions);

           assert grpNodeActions.size() == 1 : "Node is not subscribed [node=" + clusterNode + "groupId=" + groupId + ']';

           nodeActions.remove(grpNodeActions.iterator().next());
           actions.remove(grpNodeActions.iterator().next());

           if (CollectionUtils.nullOrEmpty(nodeActions)) {
               nosesSubscriptions.remove(clusterNode);
           }

           if (CollectionUtils.nullOrEmpty(actions)) {
               return null;
           }

           return actions;
       });
   }

    /**
     * Unsubscribes a node from all replication groups.
     *
     * @param clusterNode Subscriber node.
     */
    public void unsubscribeNode(ClusterNode clusterNode) {
        for (ReplicationGroupId grpId : subscriptions.keySet()) {
            unsubscribe(grpId, clusterNode);
        }
    }

    IgniteLogger LOG = Loggers.forClass(RaftServiceEventListener.class);

    /**
     * Initiates callbacks on a specific replication group.
     *
     * @param groupId Group id.
     * @param term Term.
     */
    public void onLeaderElected(ReplicationGroupId groupId, long term) {
        Set<Consumer<Long>> actions = subscriptions.get(groupId);

        if (CollectionUtils.nullOrEmpty(actions)) {
            return;
        }

        for (Consumer<Long> action : actions) {
            action.accept(term);
        }
    }

}
