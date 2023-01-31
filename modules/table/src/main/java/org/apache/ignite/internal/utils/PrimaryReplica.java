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

package org.apache.ignite.internal.utils;

import org.apache.ignite.network.ClusterNode;

/**
 * Tuple representing primary replica node with current term.
 */
public class PrimaryReplica {
    /** Primary node. */
    private final ClusterNode node;

    /** Node term. */
    private final long term;

    /**
     * Constructor.
     *
     * @param node Primary node.
     * @param term Node term.
     */
    public PrimaryReplica(ClusterNode node, long term) {
        this.node = node;
        this.term = term;
    }

    /**
     * Gets primary node.
     *
     * @return Primary node.
     */
    public ClusterNode node() {
        return node;
    }

    /**
     * Gets node term.
     *
     * @return Node term.
     */
    public long term() {
        return term;
    }
}
