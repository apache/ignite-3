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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.List;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;

/**
 * Factory to create execution target.
 */
public interface ExecutionTargetFactory {
    /**
     * Creates target from list of primary partitions.
     *
     * @param nodes List of partitions.
     * @return An execution target.
     */
    ExecutionTarget partitioned(List<NodeWithConsistencyToken> nodes);

    /**
     * Creates target from list of required nodes.
     *
     * <p>This targets specifies that in order to execute fragment properly, it must be mapped
     * to all of the nodes from the given list.
     *
     * @param nodes List of required nodes.
     * @return An execution target.
     */
    ExecutionTarget allOf(List<String> nodes);

    /**
     * Creates target from list of node options.
     *
     * <p>This targets specifies that fragment must be executed on any node from the given list,
     * but it must be mapped to exactly one node. Use this target when although fragment may be
     * executed on any subset of nodes from given list, the result set may vary from node to node.
     *
     * @param nodes List of node options.
     * @return An execution target.
     */
    ExecutionTarget oneOf(List<String> nodes);

    /**
     * Creates target from list of optional nodes.
     *
     * <p>This targets specifies that fragment may be execute on any non empty subset
     * of the given nodes. Use this target when it's known that any node from this list
     * will return exactly the same result set.
     *
     * @param nodes List of optional nodes.
     * @return An execution target.
     */
    ExecutionTarget someOf(List<String> nodes);

    /**
     * Derives a list of nodes from given target.
     *
     * @param target A target to resolve nodes from.
     * @return The list of nodes the target represents. Never null.
     */
    List<String> resolveNodes(ExecutionTarget target);

    /**
     * Derives assignments from given target.
     *
     * @param target A target to resolve assignments from.
     * @return Assignments the target represents. Never null.
     */
    Int2ObjectMap<NodeWithConsistencyToken> resolveAssignments(ExecutionTarget target);
}
