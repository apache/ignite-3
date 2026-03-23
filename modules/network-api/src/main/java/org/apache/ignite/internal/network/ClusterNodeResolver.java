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

package org.apache.ignite.internal.network;

import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * A node resolver.
 */
public interface ClusterNodeResolver {
    /**
     * Returns a cluster node consistent ID by its node ID.
     *
     * @param id Node ID.
     * @return The consistent ID; {@code null} if the node has not been discovered or is offline.
     */
    default @Nullable String getConsistentIdById(UUID id) {
        InternalClusterNode node = getById(id);

        return node != null ? node.name() : null;
    }

    /**
     * Returns a cluster node specified by its consistent ID.
     *
     * @param consistentId Consistent ID.
     * @return The node object; {@code null} if the node has not been discovered or is offline.
     */
    @Nullable InternalClusterNode getByConsistentId(String consistentId);

    /**
     * Returns a cluster node specified by its ID.
     *
     * @param id Node ID.
     * @return The node object; {@code null} if the node has not been discovered or is offline.
     */
    @Nullable InternalClusterNode getById(UUID id);
}
