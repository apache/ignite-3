/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

/**
 * A set of columns representing a key or a value chunk in a row. Provides necessary machinery to locate a column value in a concrete row.
 *
 * <p><h3>Column ordering.</h3>
 * Column instances are comparable in lexicographic order, native type first and then column name. Nullability flag is not taken into
 * account when columns are compared. Native type order guarantees fixed-len columns will prior to varlen columns, column name order
 * guarantees the same column order (for the same type) on all nodes.
 *
 * @see #COLUMN_COMPARATOR
 */
public class KeyColumns extends Columns {
    /**
     * Constructs the columns chunk. The columns will be internally sorted in write-efficient order based on {@link Column} comparison.
     *
     * @param baseSchemaIdx First column absolute index in schema.
     * @param cols          Array of columns.
     */
    KeyColumns(int baseSchemaIdx, Column... cols) {
        super(baseSchemaIdx, cols);
    }

    /** {@inheritDoc} */
    @Override
    public int nullMapSize() {
        return 0;
    }
}
