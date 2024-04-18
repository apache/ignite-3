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
import org.apache.ignite.internal.network.AbstractTopologyService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyEventHandler;
import org.jetbrains.annotations.Nullable;

/**
 * Test topology service. It does not provide any topology information, but allows to trigger events.
 */
public class TestTopologyService extends AbstractTopologyService {
    @Override
    public ClusterNode localMember() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<ClusterNode> allMembers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable ClusterNode getByAddress(NetworkAddress addr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable ClusterNode getByConsistentId(String consistentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable ClusterNode getById(String id) {
        throw new UnsupportedOperationException();
    }

    /**
     * Calls {@link TopologyEventHandler#onAppeared(ClusterNode)} on all registered event handlers.
     *
     * @param member Member.
     */
    public void fireAppearedEvent(ClusterNode member) {
        getEventHandlers().forEach(it -> it.onAppeared(member));
    }

    /**
     * Calls {@link TopologyEventHandler#onDisappeared(ClusterNode)} on all registered event handlers.
     *
     * @param member Member.
     */
    public void fireDisappearedEvent(ClusterNode member) {
        getEventHandlers().forEach(it -> it.onDisappeared(member));
    }
}
