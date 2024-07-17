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

package org.apache.ignite.internal.storage.pagememory.index.hash;

import org.apache.ignite.internal.storage.pagememory.index.common.IndexRowKey;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;

/**
 * Key to search for a {@link HashIndexRow} in the {@link HashIndexTree}.
 */
public class HashIndexRowKey implements IndexRowKey {
    private final int indexColumnsHash;

    private final IndexColumns indexColumns;

    /**
     * Constructor.
     *
     * @param indexColumnsHash Hash of the index columns.
     * @param indexColumns Index columns.
     */
    HashIndexRowKey(int indexColumnsHash, IndexColumns indexColumns) {
        this.indexColumnsHash = indexColumnsHash;

        this.indexColumns = indexColumns;
    }

    /**
     * Returns the hash of the index columns.
     */
    public int indexColumnsHash() {
        return indexColumnsHash;
    }

    @Override
    public IndexColumns indexColumns() {
        return indexColumns;
    }
}
