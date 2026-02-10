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

package org.apache.ignite.migrationtools.tablemanagement;

import java.util.Map;
import org.jetbrains.annotations.Nullable;

/** Describes the key and value types of a Table. */
public class TableTypeDescriptor {
    private final String keyClassName;

    private final String valClassName;

    @Nullable
    private final Map<String, String> keyFieldNameForColumn;

    @Nullable
    private final Map<String, String> valFieldNameForColumn;

    /**
     * Constructor.
     *
     * @param keyClassName Name of the key type.
     * @param valClassName Name of the value type.
     * @param keyFieldNameForColumn Mapping of key columns to their corresponding field names.
     *      May be null if the key class was not available.
     * @param valFieldNameForColumn Mapping of value columns to their corresponding field names.
     *      May be null if the key class was not available.
     */
    public TableTypeDescriptor(
            String keyClassName,
            String valClassName,
            @Nullable Map<String, String> keyFieldNameForColumn,
            @Nullable Map<String, String> valFieldNameForColumn
    ) {
        this.keyClassName = keyClassName;
        this.valClassName = valClassName;
        this.keyFieldNameForColumn = keyFieldNameForColumn;
        this.valFieldNameForColumn = valFieldNameForColumn;
    }

    public String keyClassName() {
        return keyClassName;
    }

    public String valClassName() {
        return valClassName;
    }

    public Map.Entry<String, String> typeHints() {
        return Map.entry(keyClassName, valClassName);
    }

    @Nullable
    public Map<String, String> keyFieldNameForColumn() {
        return keyFieldNameForColumn;
    }

    @Nullable
    public Map<String, String> valFieldNameForColumn() {
        return valFieldNameForColumn;
    }
}
