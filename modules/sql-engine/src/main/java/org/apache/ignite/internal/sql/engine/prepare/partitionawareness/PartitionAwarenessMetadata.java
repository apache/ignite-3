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

package org.apache.ignite.internal.sql.engine.prepare.partitionawareness;

import java.util.Arrays;
import org.apache.ignite.internal.tostring.S;

/**
 * Partition awareness metadata.
 *
 * @see PartitionAwarenessMetadataBuilder
 */
public final class PartitionAwarenessMetadata {

    private final int tableId;

    private final int[] indexes;

    private final int[] hash;

    /**
     * Constructor.
     *
     * @param tableId Table Id.
     * @param indexes Mapping between positions in colocation key and dynamic parameters.
     * @param hash Array of computed hashes.
     */
    public PartitionAwarenessMetadata(int tableId, int[] indexes, int[] hash) {
        this.tableId = tableId;
        this.indexes = indexes;
        this.hash = hash;
    }

    /** Return table id. */
    public int tableId() {
        return tableId;
    }

    /** Returns the number of colocation key columns. */
    public int size() {
        return indexes.length;
    }

    /**
     * Returns a mapping between positions in colocation key columns and dynamic parameter indices.
     * If a colocation key column has a value, returns {@code -1}.
     *
     * @return Mapping.
     */
    public int[] indexes() {
        return indexes;
    }

    /**
     * Returns an array of precomputed hashes.
     *
     * @return Hash array.
     */
    public int[] hash() {
        return hash;
    }



    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(PartitionAwarenessMetadata.class, this,
                "dynamicParams", Arrays.toString(indexes),
                "hash", Arrays.toString(hash)
        );
    }
}
