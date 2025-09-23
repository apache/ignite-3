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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

/**
 * RAFT server event interceptor. It is used to intercept RAFT server events of certain RAFT groups on server side, and notify about
 * them nodes that are subscribed on these events for these groups. Allows to create and remove subscriptions for nodes and groups.
 */
public class RaftServiceEventInterceptor {
    /** Callbacks by group ids. */
    private ConcurrentHashMap<ReplicationGroupId, Set<Consumer<Long>>> subscriptions = new ConcurrentHashMap<>();

    /** Callbacks by cluster nodes. */
    private ConcurrentHashMap<InternalClusterNode, Set<Consumer<Long>>> nodesSubscriptions = new ConcurrentHashMap<>();

    /**
     * Subscribes a node to notification.
     *
     * @param groupId Group id.
     * @param subscriber Subscriber node.
     * @param notifyAction Notify action.
     */
    public void subscribe(ReplicationGroupId groupId, InternalClusterNode subscriber, Consumer<Long> notifyAction) {
        subscriptions.compute(groupId, (id, actions) -> {
            if (actions == null) {
                actions = new HashSet<>();
            }

            var finalActions = actions;

            nodesSubscriptions.compute(subscriber, (node, nodeActions) -> {
                if (!nullOrEmpty(finalActions) && !nullOrEmpty(nodeActions)) {
                    return nodeActions;
                }

                if (nodeActions == null) {
                    nodeActions = new HashSet<>();
                }

                nodeActions.add(notifyAction);

                return nodeActions;
            });

            actions.add(notifyAction);

            return actions;
        });
    }

    /**
     * Unsubscribes a node.
     *
     * @param groupId Group id.
     * @param subscriber Subscriber node.
     */
    public void unsubscribe(ReplicationGroupId groupId, InternalClusterNode subscriber) {
        subscriptions.computeIfPresent(groupId, (id, actions) -> {
            nodesSubscriptions.computeIfPresent(subscriber, (node, nodeActions) -> {
                Set<Consumer<Long>> grpNodeActions = new HashSet<>(actions);

                grpNodeActions.retainAll(nodeActions);

                if (nullOrEmpty(grpNodeActions)) {
                    return nodeActions;
                }

                assert grpNodeActions.size() == 1 : "Node is not subscribed [node=" + subscriber + "groupId=" + groupId + ']';

                Consumer<Long> actionToRemove = grpNodeActions.iterator().next();

                nodeActions.remove(actionToRemove);
                actions.remove(actionToRemove);

                if (nullOrEmpty(nodeActions)) {
                    return null;
                }

                return nodeActions;
            });

            if (nullOrEmpty(actions)) {
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
    public void unsubscribeNode(InternalClusterNode clusterNode) {
        for (ReplicationGroupId grpId : subscriptions.keySet()) {
            unsubscribe(grpId, clusterNode);
        }
    }

    /**
     * Initiates callbacks on a specific replication group.
     *
     * @param groupId Group id.
     * @param term Term.
     */
    public void onLeaderElected(ReplicationGroupId groupId, long term) {
        HashSet<Consumer<Long>> actionsToInvoke = new HashSet<>();

        subscriptions.computeIfPresent(groupId, (id, actions) -> {
            actionsToInvoke.addAll(actions);

            return actions;
        });

        for (Consumer<Long> action : actionsToInvoke) {
            action.accept(term);
        }
    }
}
