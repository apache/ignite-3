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

package org.apache.ignite.compute;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.network.ClusterNode;

/**
 * Job execution target.
 *
 * <p>Determines the rules for selecting nodes to execute a job.
 */
public interface BroadcastJobTarget {
    /**
     * Creates a job target for any node from the provided collection.
     *
     * <p>This target determines that a job can be executed on any node in a given collection, but only one of them.
     * Which node is chosen is implementation defined.
     *
     * @param nodes Collection of nodes.
     * @return Job target.
     */
    static BroadcastJobTarget nodes(ClusterNode... nodes) {
        return new AllNodesBroadcastJobTarget(Set.of(nodes));
    }

    /**
     * Creates a job target for any node from the provided collection.
     *
     * <p>This target determines that a job can be executed on any node in a given collection, but only one of them.
     * Which node is chosen is implementation defined.
     *
     * @param nodes Collection of nodes.
     * @return Job target.
     */
    static BroadcastJobTarget nodes(Collection<ClusterNode> nodes) {
        return new AllNodesBroadcastJobTarget(new HashSet<>(nodes));
    }

    /**
     * Creates a job target for any node from the provided collection.
     *
     * <p>This target determines that a job can be executed on any node in a given collection, but only one of them.
     * Which node is chosen is implementation defined.
     *
     * @param nodes Collection of nodes.
     * @return Job target.
     */
    static BroadcastJobTarget nodes(Set<ClusterNode> nodes) {
        return new AllNodesBroadcastJobTarget(nodes);
    }

    /**
     * Creates a colocated job target for a specific table and key.
     *
     * <p>This target determines that a job should be executed on the same node that hosts the data for a given key of provided table.
     *
     * @param tableName Table name.
     * @param key Key.
     * @return Job target.
     */
    static BroadcastJobTarget partitioned(String tableName) {
        return new TableJobTarget(tableName);
    }
}
