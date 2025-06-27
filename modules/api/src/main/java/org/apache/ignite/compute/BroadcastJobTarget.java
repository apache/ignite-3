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
import org.apache.ignite.table.QualifiedName;

/**
 * Broadcast job execution target.
 *
 * <p>Determines the rules for selecting nodes to execute a job.
 */
public interface BroadcastJobTarget {
    /**
     * Creates a job target for all nodes from the provided collection.
     *
     * <p>This target determines that a job will be executed on all nodes from a given nodes.
     *
     * @param nodes Collection of nodes.
     * @return Job target.
     */
    static BroadcastJobTarget nodes(ClusterNode... nodes) {
        return new AllNodesBroadcastJobTarget(Set.of(nodes));
    }

    /**
     * Creates a job target for all nodes from the provided collection.
     *
     * <p>This target determines that a job will be executed on all nodes from a given collection.
     *
     * @param nodes Collection of nodes.
     * @return Job target.
     */
    static BroadcastJobTarget nodes(Collection<ClusterNode> nodes) {
        return new AllNodesBroadcastJobTarget(new HashSet<>(nodes));
    }

    /**
     * Creates a job target for all nodes from the provided collection.
     *
     * <p>This target determines that a job will be executed on all nodes from a given set.
     *
     * @param nodes Collection of nodes.
     * @return Job target.
     */
    static BroadcastJobTarget nodes(Set<ClusterNode> nodes) {
        return new AllNodesBroadcastJobTarget(nodes);
    }

    /**
     * Creates a job target for partitioned execution. For each partition in the provided table the job will be executed on a node that
     * holds the primary replica.
     *
     * @param tableName  Name of the table with SQL-parser style quotation, e.g.
     *             "tbl0" - the table "TBL0" will be looked up, "\"Tbl0\"" - "Tbl0", etc.
     * @return Job target.
     */
    static BroadcastJobTarget table(String tableName) {
        return table(QualifiedName.parse(tableName));
    }

    /**
     * Creates a job target for partitioned execution. For each partition in the provided table the job will be executed on a node that
     * holds the primary replica.
     *
     * @param tableName QualifiedName name.
     * @return Job target.
     */
    static BroadcastJobTarget table(QualifiedName tableName) {
        return new TableJobTarget(tableName);
    }
}
