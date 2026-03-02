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
import org.apache.ignite.table.QualifiedName;

/**
 * Partition awareness metadata.
 *
 * <p>The {@code indexes} array is used to describe how each element of a colocation
 * key should be interpreted during evaluation:
 * <ul>
 *   <li>If {@code indexes[i] >= 0}, then the value at position {@code i} in the
 *   colocation key should be taken from a dynamic parameter at index {@code indexes[i]}.</li>
 *   <li>If {@code indexes[i] < 0}, then the value at position {@code i} is a constant
 *   literal whose precomputed hash is stored in the {@code hash} array.
 *   The corresponding hash value is located at index {@code -(indexes[i] + 1)} in the
 *   {@code hash} array.</li>
 * </ul>
 * In other words:
 * <pre>
 *   indexes[i] >= 0         => use dynamicParam[indexes[i]]
 *   indexes[i] < 0          => use hash[-(indexes[i] + 1)]
 * </pre>
 */
public final class PartitionAwarenessMetadata {

    private final int tableId;

    private final int[] indexes;

    private final int[] hash;

    private final DirectTxMode directTxMode;

    private final QualifiedName tableName;

    /**
     * Constructor.
     *
     * @param tableId Table Id.
     * @param indexes Mapping between positions in colocation key and dynamic parameters.
     * @param hash Array of computed hashes.
     * @param directTxMode The level of support for direct transaction.
     * @param tableName Table name.
     */
    public PartitionAwarenessMetadata(
            int tableId,
            int[] indexes,
            int[] hash,
            DirectTxMode directTxMode,
            QualifiedName tableName
    ) {
        this.tableId = tableId;
        this.indexes = indexes;
        this.hash = hash;
        this.directTxMode = directTxMode;
        this.tableName = tableName;
    }

    /** Return table id. */
    public int tableId() {
        return tableId;
    }

    /** Return table name. */
    public QualifiedName tableName() {
        return tableName;
    }

    /** Returns the number of colocation key columns. */
    public int size() {
        return indexes.length;
    }

    /**
     * Returns a mapping between positions in colocation key columns and dynamic parameter indices.
     * If a colocation key column has a value, returns a negated 1-based index into the hash array.
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

    /** Returns the level of support for direct transaction. */
    public DirectTxMode directTxMode() {
        return directTxMode;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(PartitionAwarenessMetadata.class, this,
                "indexes", Arrays.toString(indexes),
                "hash", Arrays.toString(hash),
                "directTxMode", directTxMode
        );
    }
}
