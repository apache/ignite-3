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

/**
 * CacheKey.
 * The class uses to distinguish different query plans which could be various for the same query text but different context. As example such
 * context could be schema name, dynamic parameters, and so on...
 */
public class CacheKey {
    private static final Class[] EMPTY_CLASS_ARRAY = {};

    private final String schemaName;

    private final String query;

    private final Class[] paramTypes;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param query      Query string.
     * @param params     Dynamic parameters.
     */
    public CacheKey(String schemaName, String query, Object[] params) {
        this.schemaName = schemaName;
        this.query = query;
        this.paramTypes = params.length == 0
                ? EMPTY_CLASS_ARRAY
                : Arrays.stream(params).map(p -> (p != null) ? p.getClass() : Void.class).toArray(Class[]::new);
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

        CacheKey cacheKey = (CacheKey) o;

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
        int result = schemaName.hashCode();
        result = 31 * result + query.hashCode();
        result = 31 * result + Arrays.deepHashCode(paramTypes);
        return result;
    }
}
