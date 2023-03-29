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

package org.apache.ignite.internal.catalog.commands;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.sql.ColumnType;

/** Defines a particular column within table. */
public class ColumnParams implements Serializable {
    private static final long serialVersionUID = 5602599481844743521L;

    private final String name;

    private final ColumnType type;

    private final boolean nullable;

    private final DefaultValue defaultValueDefinition;

    /** Creates a column definition. */
    public ColumnParams(String name, ColumnType type, DefaultValue defaultValueDefinition, boolean nullable) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        this.defaultValueDefinition = Objects.requireNonNull(defaultValueDefinition, "defaultValueDefinition");
        this.nullable = nullable;
    }

    /**
     * Get column's name.
     */
    public String name() {
        return name;
    }

    /**
     * Get column's type.
     */
    public ColumnType type() {
        return type;
    }

    /**
     * Returns default value definition.
     *
     * @param <T> Desired subtype of the definition.
     * @return Default value definition.
     */
    @SuppressWarnings("unchecked")
    public <T extends DefaultValue> T defaultValueDefinition() {
        return (T) defaultValueDefinition;
    }

    /**
     * Get nullable flag: {@code true} if this column accepts nulls.
     */
    public boolean nullable() {
        return nullable;
    }

    /**
     * Get column's precision.
     */
    public Integer precision() {
        return null;
    }

    /**
     * Get column's scale.
     */
    public Integer scale() {
        return null;
    }
}
