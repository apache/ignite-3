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

package org.apache.ignite.internal.client.table;

import org.apache.ignite.internal.client.proto.ClientDataType;

/**
 * Schema column.
 */
public class ClientColumn {
    /** Column name. */
    private final String name;

    /** Column type code (see {@link ClientDataType}). */
    private final int type;

    /** Nullable flag. */
    private final boolean nullable;

    /** Key column flag. */
    private final boolean isKey;

    /** Key column flag. */
    private final boolean isColocation;

    /** Index of the column in the schema. */
    private final int schemaIndex;

    /** value scale, if applicable. */
    private final int scale;

    /** value precision, if applicable. */
    private final int precision;

    /**
     * Constructor.
     *
     * @param name         Column name.
     * @param type         Column type code.
     * @param nullable     Nullable flag.
     * @param isKey        Key column flag.
     * @param isColocation Colocation column flag.
     * @param schemaIndex  Index of the column in the schema.
     */
    public ClientColumn(String name, int type, boolean nullable, boolean isKey, boolean isColocation, int schemaIndex) {
        this(name, type, nullable, isKey, isColocation, schemaIndex, 0, 0);
    }

    /**
     * Constructor.
     *
     * @param name        Column name.
     * @param type        Column type code.
     * @param nullable    Nullable flag.
     * @param isKey       Key column flag.
     * @param schemaIndex Index of the column in the schema.
     * @param scale       Scale of the column, if applicable.
     */
    public ClientColumn(
            String name,
            int type,
            boolean nullable, boolean isKey,
            boolean isColocation,
            int schemaIndex,
            int scale,
            int precision) {
        assert name != null;
        assert schemaIndex >= 0;

        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.isKey = isKey;
        this.isColocation = isColocation;
        this.schemaIndex = schemaIndex;
        this.scale = scale;
        this.precision = precision;
    }

    public String name() {
        return name;
    }

    /**
     * Client data type, see {@link ClientDataType}.
     *
     * @return Data type code.
     */
    public int type() {
        return type;
    }

    /**
     * Gets a value indicating whether this column is nullable.
     *
     * @return Value indicating whether this column is nullable.
     */
    public boolean nullable() {
        return nullable;
    }

    /**
     * Gets a value indicating whether this column is a part of key.
     *
     * @return Value indicating whether this column is a part of key.a part of key
     */
    public boolean key() {
        return isKey;
    }

    /**
     * Gets a value indicating whether this column is a part of colocation key.
     *
     * @return Value indicating whether this column is a part of colocation key.
     */
    public boolean colocation() {
        return isColocation;
    }

    /**
     * Gets the index of the column in the schema.
     *
     * @return Schema index.
     */
    public int schemaIndex() {
        return schemaIndex;
    }

    /**
     * Gets the decimal scale of the column, if applicable.
     *
     * @return Decimal scale.
     */
    public int scale() {
        return scale;
    }

    /**
     * Gets the precision of the column, if applicable.
     *
     * @return Precision.
     */
    public int precision() {
        return precision;
    }
}
