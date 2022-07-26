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

package org.apache.ignite.internal.schema.definition;

import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.DefaultValueDefinition;

/**
 * Table column.
 */
public class ColumnDefinitionImpl implements ColumnDefinition {
    /** Column name. */
    private final String name;

    /** Column type. */
    private final ColumnType type;

    /** Nullable flag. */
    private final boolean nullable;

    private final DefaultValueDefinition defaultValueDefinition;

    /**
     * Constructor.
     *
     * @param name Column name.
     * @param type Column type.
     * @param nullable Nullability flag.
     * @param defaultValueDefinition Default value.
     */
    public ColumnDefinitionImpl(String name, ColumnType type, boolean nullable, DefaultValueDefinition defaultValueDefinition) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.defaultValueDefinition = defaultValueDefinition;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public <T extends DefaultValueDefinition> T defaultValueDefinition() {
        return (T) defaultValueDefinition;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ColumnDefinitionImpl.class, this);
    }
}
