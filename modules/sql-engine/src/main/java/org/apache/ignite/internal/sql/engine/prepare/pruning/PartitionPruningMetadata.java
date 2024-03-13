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

package org.apache.ignite.internal.sql.engine.prepare.pruning;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import java.io.Serializable;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Partition pruning metadata includes partition pruning information for each operator of a physical plan that supports partition pruning.
 *
 * @see PartitionPruningColumns
 * @see PartitionPruningMetadataExtractor
 */
public class PartitionPruningMetadata implements Serializable {

    private static final long serialVersionUID = 0;

    /** Empty metadata. */
    public static final PartitionPruningMetadata EMPTY = new PartitionPruningMetadata(Long2ObjectMaps.emptyMap());

    private final Long2ObjectMap<PartitionPruningColumns> data;

    /** Constructor. */
    public PartitionPruningMetadata(Long2ObjectMap<PartitionPruningColumns> data) {
        this.data = Long2ObjectMaps.unmodifiable(data);
    }

    /** Return metadata for operator with the given sourceId, or {@code null} if there is no metadata for it. */
    public @Nullable PartitionPruningColumns get(long sourceId) {
        return data.get(sourceId);
    }

    /** A map of operators with their metadata. */
    public Long2ObjectMap<PartitionPruningColumns> data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(PartitionPruningMetadata.class, this, "data", data);
    }
}
