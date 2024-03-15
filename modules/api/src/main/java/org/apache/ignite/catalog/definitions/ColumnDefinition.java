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

import java.util.Objects;
import org.apache.ignite.catalog.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Definition of the column. The type of the column is defined either with the {@link ColumnType} or with the string representing the type.
 */
public class ColumnDefinition {
    private final String name;

    private final ColumnType<?> type;

    private final String definition;

    private ColumnDefinition(String name, @Nullable ColumnType<?> type, @Nullable String definition) {
        this.name = name;
        this.type = type;
        this.definition = definition;
    }

    /**
     * Creates a column definition with the provided type.
     *
     * @param name Column name.
     * @param type Type of the column.
     * @return Constructed column definition.
     */
    public static ColumnDefinition column(String name, ColumnType<?> type) {
        Objects.requireNonNull(name, "Column name must not be null.");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Column name must not be blank.");
        }
        Objects.requireNonNull(type, "Column type must not be null.");

        return new ColumnDefinition(name, type, null);
    }

    /**
     * Creates a column definition with the provided type definition.
     *
     * @param name Column name.
     * @param definition Definition of the type of the column.
     * @return Constructed column definition.
     */
    public static ColumnDefinition column(String name, String definition) {
        Objects.requireNonNull(name, "Column name must not be null.");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Column name must not be blank.");
        }
        Objects.requireNonNull(definition, "Column definition must not be null.");
        if (definition.isBlank()) {
            throw new IllegalArgumentException("Column definition must not be blank.");
        }

        return new ColumnDefinition(name, null, definition);
    }

    /**
     * Returns column name.
     *
     * @return Column name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns type of the column.
     *
     * @return Type of the column if present.
     */
    public @Nullable ColumnType<?> type() {
        return type;
    }

    /**
     * Returns string definition of the column type.
     *
     * @return String definition of the column type.
     */
    public @Nullable String definition() {
        return definition;
    }
}
