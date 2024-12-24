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

import java.util.Set;
import org.apache.ignite.network.ClusterNode;

public interface BroadcastJobTarget {
    /**
     * Creates a broadcast job target for a specific nodes. The jobs will be executed on all specified nodes.
     *
     * @param nodes Nodes.
     * @return Broadcast job target.
     */
    static BroadcastJobTarget nodes(ClusterNode... nodes) {
        return new NodesBroadcastJobTarget(Set.of(nodes));
    }

    /**
     * Creates a broadcast job target for a specific table. The jobs will be executed on all nodes holding the table partitions,
     * one job per node.
     *
     * @param tableName Table name.
     * @return Broadcast job target.
     */
    static BroadcastJobTarget table(String tableName) {
        return new TableBroadcastJobTarget(tableName);
    }
}
