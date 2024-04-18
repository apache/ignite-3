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
import java.io.Serializable;
import java.util.List;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Descriptions of the query fragment.
 *
 * <p>Mostly describes topology of the fragment, but may contain other attributes required for execution.
 */
public class FragmentDescription implements Serializable {
    private static final long serialVersionUID = 0L;

    private final long fragmentId;
    private final boolean prefetch;
    private final Long2ObjectMap<ColocationGroup> groupsBySourceId;
    private final @Nullable ColocationGroup target;
    private final @Nullable Long2ObjectMap<List<String>> sourcesByExchangeId;
    private final @Nullable PartitionPruningMetadata pruningMetadata;

    /**
     * Constructor.
     *
     * @param fragmentId An identifier of the fragment.
     * @param prefetch A flag denoting whether this fragment may be executed in advance.
     * @param groupsBySourceId A mapping of colocation groups by source id.
     * @param target A target group this fragment should stream data to.
     * @param sourcesByExchangeId A mapping of sources this fragment should receive data from.
     */
    public FragmentDescription(
            long fragmentId,
            boolean prefetch,
            Long2ObjectMap<ColocationGroup> groupsBySourceId,
            @Nullable ColocationGroup target,
            @Nullable Long2ObjectMap<List<String>> sourcesByExchangeId,
            @Nullable PartitionPruningMetadata pruningMetadata
    ) {
        this.fragmentId = fragmentId;
        this.prefetch = prefetch;
        this.groupsBySourceId = groupsBySourceId;
        this.target = target;
        this.sourcesByExchangeId = sourcesByExchangeId;
        this.pruningMetadata = pruningMetadata;
    }

    /** Returns {@code true} if it's safe to execute this fragment in advance. */
    public boolean prefetch() {
        return prefetch;
    }

    /**
     * Get fragment id.
     */
    public long fragmentId() {
        return fragmentId;
    }

    /**
     * Get target.
     */
    public @Nullable ColocationGroup target() {
        return target;
    }

    /**
     * Get remotes.
     */
    public @Nullable List<String> remotes(long exchangeId) {
        if (sourcesByExchangeId == null) {
            return null;
        }

        return sourcesByExchangeId.get(exchangeId);
    }

    /**
     * Get mapping.
     */
    public @Nullable ColocationGroup group(long sourceId) {
        return groupsBySourceId.get(sourceId);
    }

    /**
     * Returns partition pruning metadata.
     */
    public @Nullable PartitionPruningMetadata partitionPruningMetadata() {
        return pruningMetadata;
    }
}
