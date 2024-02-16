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

package org.apache.ignite.catalog.definitions;

import java.util.List;
import org.apache.ignite.catalog.IndexType;
import org.jetbrains.annotations.Nullable;

/**
 * Definition of the {@code CREATE INDEX} statement.
 */
public class IndexDefinition {
    @Nullable
    private final String name;

    private final IndexType type;

    private final List<ColumnSorted> columns;

    IndexDefinition(@Nullable String name, IndexType type, List<ColumnSorted> columns) {
        this.name = name;
        this.type = type;
        this.columns = columns;
    }

    /**
     * Returns index name.
     *
     * @return Index name, or {@code null} if it should be autogenerated.
     */
    public @Nullable String name() {
        return name;
    }

    /**
     * Returns index type.
     *
     * @return Index type.
     */
    public IndexType type() {
        return type;
    }

    /**
     * Returns list of column references to include in the index.
     *
     * @return List of column references to include in the index.
     */
    public List<ColumnSorted> columns() {
        return columns;
    }
}
