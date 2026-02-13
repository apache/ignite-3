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

package org.apache.ignite.internal.sql.engine.prepare;

import java.util.Arrays;
import org.apache.ignite.sql.ColumnType;

/**
 * Key without catalog version.
 */
public class CacheKeyBase {
    static final ColumnType[] EMPTY_CLASS_ARRAY = {};

    private final String schemaName;

    private final String query;

    private final ColumnType[] paramTypes;

    private int hashCode = 0;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param query Query string.
     * @param paramTypes Types of all dynamic parameters, no any type can be {@code null}.
     */
    public CacheKeyBase(String schemaName, String query, ColumnType[] paramTypes) {
        this.schemaName = schemaName;
        this.query = query;
        this.paramTypes = paramTypes;
    }

    public CacheKeyBase(CacheKey key) {
        this.schemaName = key.schemaName();
        this.query = key.query();
        this.paramTypes = key.paramTypes();
    }

    String schemaName() {
        return schemaName;
    }

    ColumnType[] paramTypes() {
        return paramTypes;
    }

    String query() {
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CacheKeyBase cacheKey = (CacheKeyBase) o;

        if (!schemaName.equals(cacheKey.schemaName)) {
            return false;
        }
        if (!query.equals(cacheKey.query)) {
            return false;
        }

        return Arrays.deepEquals(paramTypes, cacheKey.paramTypes);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            int result = schemaName.hashCode();
            result = 31 * result + query.hashCode();
            result = 31 * result + Arrays.deepHashCode(paramTypes);

            hashCode = result;
        }

        return hashCode;
    }

    public CacheKey withCatalogVersion(int version) {
        return new CacheKey(version, schemaName, query, paramTypes);
    }
}
