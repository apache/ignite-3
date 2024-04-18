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

import java.util.Objects;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.ClusterNode;

/**
 * Tuple representing primary replica node with its enlistment consistency token.
 */
public class PrimaryReplica {
    /** Primary replica node. */
    private final ClusterNode node;

    /** Enlistment consistency token. */
    private final long enlistmentConsistencyToken;

    /**
     * Constructor.
     *
     * @param node Primary replica node.
     * @param enlistmentConsistencyToken Enlistment consistency token.
     */
    public PrimaryReplica(ClusterNode node, long enlistmentConsistencyToken) {
        this.node = node;
        this.enlistmentConsistencyToken = enlistmentConsistencyToken;
    }

    /**
     * Gets primary replica node.
     *
     * @return Primary replica node.
     */
    public ClusterNode node() {
        return node;
    }

    /**
     * Gets enlistment consistency token.
     *
     * @return Enlistment consistency token.
     */
    public long enlistmentConsistencyToken() {
        return enlistmentConsistencyToken;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrimaryReplica that = (PrimaryReplica) o;
        return enlistmentConsistencyToken == that.enlistmentConsistencyToken && Objects.equals(node, that.node);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(node, enlistmentConsistencyToken);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
