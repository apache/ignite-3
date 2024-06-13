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

package org.apache.ignite.internal.sql.engine.schema;

import org.apache.ignite.internal.type.NativeType;
import org.jetbrains.annotations.Nullable;

/**
 * An object describing a particular column in a table.
 */
public interface ColumnDescriptor {
    /** Returns {@code true} if this column accepts a null value. */
    boolean nullable();

    /** Returns {@code true} if this column is part of the primary key. */
    boolean key();

    /** Returns {@code true} if this column should not be expanded in query until user explicitly specify it as part of the statement. */
    boolean hidden();

    /** Returns {@code true} if this column should not be stored. */
    default boolean system() {
        return false;
    }

    /** Returns the strategy to follow when generating value for column not specified in the INSERT statement. */
    DefaultValueStrategy defaultStrategy();

    /** Returns the name of the column. */
    String name();

    /** Returns 0-based index of the column according to a schema defined by a user. */
    int logicalIndex();

    /** Returns the type of this column in a storage. */
    NativeType physicalType();

    /** Returns the value to use for column not specified in the INSERT statement. */
    @Nullable Object defaultValue();
}
