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

import java.util.Objects;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.storage.LogManager;

/**
 * Used to get JRaft node when it's created and later provide access to JRaft services accessible through it.
 * {@link JraftServerImpl} injects the node.
 */
public class JraftNodeAccess {
    // TODO: IGNITE-18011 - remove this when a better way to obtain snapshot meta is available

    private volatile Node node;

    /**
     * Accepts and saves a node for later usage.
     *
     * @param node JRaft node.
     */
    public void node(Node node) {
        this.node = node;
    }

    /**
     * Returns a {@link LogManager} from the node.
     *
     * @return LogManager instance.
     */
    public LogManager logManager() {
        NodeImpl nodeImpl = (NodeImpl) node;

        if (nodeImpl == null) {
            throw new IllegalStateException("No node provided");
        }

        return Objects.requireNonNull(nodeImpl.logManager(), "LogManager is not instantiated yet");
    }
}
