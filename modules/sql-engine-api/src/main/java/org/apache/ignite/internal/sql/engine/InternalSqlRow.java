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

package org.apache.ignite.internal.sql.engine;

import org.apache.ignite.internal.schema.BinaryTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Internal representation of SQL row.
 */
public interface InternalSqlRow {
    /**
     * Get column value from the row by index.
     *
     * @param idx Index of a column.
     * @return Value of column. Returns value can be {@code null}.
     */
    @Nullable Object get(int idx);

    /**
     * Count of filed in the row.
     */
    int fieldCount();

    /**
     * Returns representation of the row as {@code BinaryTuple}.
     */
    BinaryTuple asBinaryTuple();
}
