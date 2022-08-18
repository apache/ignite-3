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

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Description of the index.
 */
public class IndexDescriptor {
    private final String name;

    private final List<String> columns;

    /**
     * Constructs a index description.
     *
     * @param name Name of the index.
     * @param columns A list of indexed columns. Must not contains duplicates.
     * @throws IllegalArgumentException If columns list contains duplicates.
     */
    public IndexDescriptor(String name, List<String> columns) {
        if (new HashSet<>(columns).size() != columns.size()) {
            throw new IllegalArgumentException("Indexed columns should be unique");
        }

        this.name = Objects.requireNonNull(name, "name");
        this.columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
    }

    /** Returns name of the described index. */
    public String name() {
        return name;
    }

    /** Returns indexed columns. */
    public List<String> columns() {
        return columns;
    }
}
