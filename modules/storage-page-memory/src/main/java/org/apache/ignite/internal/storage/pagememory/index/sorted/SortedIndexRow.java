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

package org.apache.ignite.internal.storage.pagememory.index.sorted;

import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;

/**
 * {@link IndexRow} implementation used in the {@link SortedIndexTree}.
 */
public class SortedIndexRow extends SortedIndexRowKey {
    /** Row id. */
    private final RowId rowId;

    /**
     * Constructor.
     *
     * @param indexColumns Index columns.
     * @param rowId Row id.
     */
    public SortedIndexRow(IndexColumns indexColumns, RowId rowId) {
        super(indexColumns);

        this.rowId = rowId;
    }

    /**
     * Returns a row id of the row.
     */
    public RowId rowId() {
        return rowId;
    }
}
