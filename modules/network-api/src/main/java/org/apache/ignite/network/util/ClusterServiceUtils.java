/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.network.util;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;

/**
 * Various utilities related to {@link ClusterService}.
 */
public class ClusterServiceUtils {
    private ClusterServiceUtils() {
    }

    /**
     * Resolves given node names (a.k.a. consistent IDs) into {@link ClusterNode}s.
     *
     * @param consistentIds consistent IDs.
     * @return list of resolved {@code ClusterNode}s.
     * @throws IllegalArgumentException if any of the given nodes are not present in the physical topology.
     */
    public static List<ClusterNode> resolveNodes(ClusterService clusterService, Collection<String> consistentIds) {
        return consistentIds.stream()
                .map(consistentId -> {
                    ClusterNode node = clusterService.topologyService().getByConsistentId(consistentId);

                    if (node == null) {
                        throw new IllegalArgumentException(String.format(
                                "Node \"%s\" is not present in the physical topology", consistentId
                        ));
                    }

                    return node;
                })
                .collect(Collectors.toList());
    }
}
