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

/**
 * A class that returns a single {@link InternalClusterNode} for every request.
 */
public class SingleClusterNodeResolver implements ClusterNodeResolver {

    private final InternalClusterNode clusterNode;

    /**
     * Constructor.
     *
     * @param clusterNode Default cluster node that will be returned as a result of all method calls.
     */
    public SingleClusterNodeResolver(InternalClusterNode clusterNode) {
        this.clusterNode = clusterNode;
    }

    @Override
    public InternalClusterNode getByConsistentId(String consistentId) {
        return clusterNode;
    }

    @Override
    public InternalClusterNode getById(UUID id) {
        return clusterNode;
    }
}
