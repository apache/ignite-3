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

package org.apache.ignite.internal.schema;

import org.apache.ignite.internal.binarytuple.BinaryTuple;

/**
 * Class for extracting a subset of columns from {@code BinaryRow}s.
 */
@FunctionalInterface
public interface ColumnsExtractor {
    /**
     * Extracts a subset of columns from a given {@code BinaryRow}.
     *
     * @param row Row with data (both key and value).
     * @return Subset of columns, packed into a {@code BinaryTuple}.
     */
    BinaryTuple extractColumns(BinaryRow row);

    /**
     * Checks whether index columns match the given table row.
     *
     * <p>The default implementation extracts columns and compares the resulting byte buffer.
     * Implementations may override this to avoid the allocation.
     *
     * @param tableRow Row with data from table.
     * @param indexColumns Binary tuple representation of indexed columns.
     * @return {@code true} if the index columns match the table row, {@code false} otherwise.
     */
    default boolean columnsMatch(BinaryRow tableRow, BinaryTuple indexColumns) {
        return extractColumns(tableRow).byteBuffer().equals(indexColumns.byteBuffer());
    }
}
