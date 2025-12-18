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

package org.apache.ignite.internal.network.file;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.network.AbstractTopologyService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * Test topology service. It does not provide any topology information, but allows to trigger events.
 */
public class TestTopologyService extends AbstractTopologyService {
    @Override
    public InternalClusterNode localMember() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<InternalClusterNode> allMembers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<InternalClusterNode> logicalTopologyMembers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable InternalClusterNode getByAddress(NetworkAddress addr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable InternalClusterNode getByConsistentId(String consistentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable InternalClusterNode getById(UUID id) {
        throw new UnsupportedOperationException();
    }

    /**
     * Calls {@link TopologyEventHandler#onAppeared(InternalClusterNode)} on all registered event handlers.
     *
     * @param member Member.
     */
    public void fireAppearedEvent(InternalClusterNode member) {
        getEventHandlers().forEach(it -> it.onAppeared(member));
    }

    /**
     * Calls {@link TopologyEventHandler#onDisappeared(InternalClusterNode)} on all registered event handlers.
     *
     * @param member Member.
     */
    public void fireDisappearedEvent(InternalClusterNode member) {
        getEventHandlers().forEach(it -> it.onDisappeared(member));
    }

    @Override
    public void onJoined(InternalClusterNode node, long topologyVersion) {
    }

    @Override
    public void onLeft(InternalClusterNode node, long topologyVersion) {
    }
}
