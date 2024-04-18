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

package org.apache.ignite.internal.sql.engine.exec;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.ArrayList;
import java.util.List;

/**
 * Returns a list of partitions for a particular scan operation.
 */
@FunctionalInterface
public interface PartitionProvider<RowT> {
    /**
     * Returns a list of partitions for a scan operation.
     *
     * @param ctx Execution context.
     * @return a list of partitions.
     */
    List<PartitionWithConsistencyToken> getPartitions(ExecutionContext<RowT> ctx);

    /** Returns a partition provider that always returns the given list of partitions. */
    static <RowT> PartitionProvider<RowT> fromPartitions(List<PartitionWithConsistencyToken> partitions) {
        return ctx -> partitions;
    }

    /**
     * Returns a list of partitions that belong to to the given node.
     *
     * @param assignments Assignments.
     * @param nodeName node name.
     *
     * @return List of partitions.
     */
    static List<PartitionWithConsistencyToken> partitionsForNode(Int2ObjectMap<NodeWithConsistencyToken> assignments, String nodeName) {
        List<PartitionWithConsistencyToken> result = new ArrayList<>();

        for (int i = 0; i < assignments.size(); i++) {
            NodeWithConsistencyToken a = assignments.get(i);
            if (a.name().equals(nodeName)) {
                result.add(new PartitionWithConsistencyToken(i, a.enlistmentConsistencyToken()));
            }
        }

        return result;
    }
}
