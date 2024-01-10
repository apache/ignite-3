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

package org.apache.ignite.internal.compute;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;

/**
 * This is a workaround for implementing add and remove event handlers for node left events.
 * todo:  remove and use TopologyService after <a href="https://issues.apache.org/jira/browse/IGNITE-14519">IGNITE-14519</a>.
 */
class NodeLeftEventsSource {
    private final List<Consumer<ClusterNode>> handlers;

    NodeLeftEventsSource(TopologyService delegate) {
        this.handlers = new CopyOnWriteArrayList<>();

        delegate.addEventHandler(new NodeLeftTopologyEventHandler());
    }

    void addEventHandler(Consumer<ClusterNode> onNodeLeftHandler) {
        handlers.add(onNodeLeftHandler);
    }

    void removeEventHandler(Consumer<ClusterNode> onNodeLeftHandler) {
        handlers.remove(onNodeLeftHandler);
    }

    private class NodeLeftTopologyEventHandler implements TopologyEventHandler {
        @Override
        public void onDisappeared(ClusterNode member) {
            handlers.forEach(handler -> handler.accept(member));
        }
    }
}
