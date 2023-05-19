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

package org.apache.ignite.internal.table.distributed.index;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;

/**
 * Row to build the index.
 */
public class BuildIndexRow {
    private final RowId rowId;

    private final BinaryRow binaryRow;

    /**
     * Constructor.
     *
     * @param rowId Row ID.
     * @param binaryRow Binary row to insert.
     */
    public BuildIndexRow(RowId rowId, BinaryRow binaryRow) {
        this.rowId = rowId;
        this.binaryRow = binaryRow;
    }

    /**
     * Returns row ID.
     */
    public RowId getRowId() {
        return rowId;
    }

    /**
     * Returns binary row to insert.
     */
    public BinaryRow getBinaryRow() {
        return binaryRow;
    }
}
