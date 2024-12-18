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

package org.apache.ignite.internal.cluster.management;

import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.ignite.internal.network.AbstractTopologyService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

class PhysicalTopologySimulator extends AbstractTopologyService {
    private final Set<ClusterNode> clusterMembers;
    private final ClusterNode localMember;

    PhysicalTopologySimulator(Set<ClusterNode> initialTopology) {
        this.clusterMembers = new CopyOnWriteArraySet<>(initialTopology);
        this.localMember = initialTopology.stream().findFirst().orElse(null);
    }

    @Override
    public ClusterNode localMember() {
        if (localMember == null) {
            throw new NoSuchElementException();
        }
        return localMember;
    }

    @Override
    public Collection<ClusterNode> allMembers() {
        return Collections.unmodifiableSet(clusterMembers);
    }

    @Override
    public @Nullable ClusterNode getByAddress(NetworkAddress addr) {
        return clusterMembers.stream().filter((node) -> node.address().equals(addr)).findFirst().orElse(null);
    }

    @Override
    public @Nullable ClusterNode getByConsistentId(String consistentId) {
        return clusterMembers.stream().filter((node) -> node.id().toString().equals(consistentId)).findFirst().orElse(null);
    }

    @Override
    public @Nullable ClusterNode getById(UUID id) {
        return clusterMembers.stream().filter((node) -> node.id().equals(id)).findFirst().orElse(null);
    }

    void addNode(ClusterNode member) {
        boolean nodeAdded = clusterMembers.add(member);
        if (nodeAdded) {
            getEventHandlers().forEach(it -> it.onAppeared(member));
        }
    }

    void removeNode(ClusterNode member) {
        boolean nodeRemoved = clusterMembers.remove(member);
        if (nodeRemoved) {
            getEventHandlers().forEach(it -> it.onDisappeared(member));
        }
    }

    boolean hasNoEventHandlers() {
        return getEventHandlers().isEmpty();
    }
}
