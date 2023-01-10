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

package org.apache.ignite.internal.storage;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.TableRow;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper that holds both {@link BinaryRow} and {@link RowId}. {@link BinaryRow} is null for tombstones.
 */
public class TableRowAndRowId {
    /** Binary row. */
    private final @Nullable TableRow tableRow;

    /** Row id. */
    private final RowId rowId;

    /**
     * Constructor.
     *
     * @param tableRow Binary row.
     * @param rowId Row id.
     */
    public TableRowAndRowId(@Nullable TableRow tableRow, RowId rowId) {
        this.tableRow = tableRow;
        this.rowId = rowId;
    }

    public @Nullable TableRow tableRow() {
        return tableRow;
    }

    public RowId rowId() {
        return rowId;
    }
}
