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

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.LongList;
import java.util.List;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;

/**
 * Helper class containing set of utility methods for mapping-related needs.
 */
public final class MappingUtils {
    /**
     * Creates a {@link MappedFragment} representing a single-node mapping of a query fragment.
     *
     * <p>This is useful in scenarios where the execution of the fragment is restricted to a specific node.
     *
     * <p>Created mapping is fake, and should not be used for consequent execution. Created mapping is suitable
     * for EXPLAIN though.
     *
     * @param nodeName The name of the node where the fragment should be executed.
     * @param root The root relational expression of the fragment.
     * @return A {@link MappedFragment} object encapsulating the fragment and its colocation group.
     */
    public static MappedFragment createSingleNodeMapping(String nodeName, IgniteRel root) {
        List<ColocationGroup> groups = List.of(
                new ColocationGroup(LongList.of(0), List.of(nodeName), Int2ObjectMaps.emptyMap())
        );

        return new MappedFragment(
                new Fragment(0, false, root, List.of(), Long2ObjectMaps.emptyMap(), List.of()), groups, null, null, null
        );
    }

    private MappingUtils() {
        throw new AssertionError("Should not be called");
    }
}
