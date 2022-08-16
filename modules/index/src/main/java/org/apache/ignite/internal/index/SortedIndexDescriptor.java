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

package org.apache.ignite.internal.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Description of the sorted index.
 */
public class SortedIndexDescriptor extends IndexDescriptor {
    private final Map<String, ColumnCollation> collations;

    /**
     * Constructs a index description.
     *
     * @param name Name of the index.
     * @param columns A list of indexed columns. Must not contains duplicates.
     * @param collations A list of columns collations. Must be the same size as columns list.
     * @throws IllegalArgumentException If columns list contains duplicates or columns size doesn't match the collations size.
     */
    public SortedIndexDescriptor(
            String name,
            List<String> columns,
            List<ColumnCollation> collations
    ) {
        super(name, columns);

        Objects.requireNonNull(collations, "collations");

        if (collations.size() != columns.size()) {
            throw new IllegalArgumentException("Both collection of columns and collations should have the same size");
        }

        Map<String, ColumnCollation> tmp = new HashMap<>();

        for (int i = 0; i < columns().size(); i++) {
            ColumnCollation prev = tmp.putIfAbsent(columns().get(i), collations.get(i));

            assert prev == null;
        }

        this.collations = Map.copyOf(tmp);
    }

    /**
     * Returns collation for the given column.
     *
     * @param columnName A column of interest.
     * @return Collation of the column or {@code null} if the given column is not part of this index.
     */
    public @Nullable ColumnCollation collation(String columnName) {
        return collations.get(columnName);
    }
}
