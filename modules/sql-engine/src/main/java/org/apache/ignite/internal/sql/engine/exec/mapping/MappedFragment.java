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

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.jetbrains.annotations.Nullable;

/**
 * A result of a mapping.
 *
 * <p>Encloses original fragment and information describing topology of a given fragment. That is,
 * the list of nodes this fragment should be executed on, target group this fragment will send rows to,
 * list of rows by exchange id to provide every receiver in this fragment with list of nodes the receiver
 * expects rows from, and colocation group for every source in this fragment.
 */
public class MappedFragment {
    private final Fragment fragment;
    private final List<String> nodes;
    private final Long2ObjectMap<ColocationGroup> groupsBySourceId;
    private final @Nullable Long2ObjectMap<List<String>> sourcesByExchangeId;
    private final @Nullable ColocationGroup target;

    /** Constructor. */
    MappedFragment(
            Fragment fragment,
            List<ColocationGroup> groups,
            @Nullable Long2ObjectMap<List<String>> sourcesByExchangeId,
            @Nullable ColocationGroup target
    ) {
        this.fragment = fragment;

        Set<String> nodes = new HashSet<>();

        Long2ObjectMap<ColocationGroup> groupsBySourceId = new Long2ObjectOpenHashMap<>();
        for (ColocationGroup group : groups) {
            nodes.addAll(group.nodeNames());

            for (long sourceId : group.sourceIds()) {
                groupsBySourceId.put(sourceId, group);
            }
        }

        this.nodes = List.copyOf(nodes);
        this.groupsBySourceId = groupsBySourceId;
        this.sourcesByExchangeId = sourcesByExchangeId;
        this.target = target;
    }

    public Fragment fragment() {
        return fragment;
    }

    public List<String> nodes() {
        return nodes;
    }

    public Long2ObjectMap<ColocationGroup> groupsBySourceId() {
        return groupsBySourceId;
    }

    public @Nullable ColocationGroup target() {
        return target;
    }

    public @Nullable Long2ObjectMap<List<String>> sourcesByExchangeId() {
        return sourcesByExchangeId;
    }
}
