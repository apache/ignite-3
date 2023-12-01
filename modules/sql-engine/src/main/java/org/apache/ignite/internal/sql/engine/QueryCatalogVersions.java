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

package org.apache.ignite.internal.sql.engine;

import java.util.Map;

/**
 * Contains information about both the base catalog version (corresponding to the transaction start timestamp)
 * and overrides specific to the current query. Overrides are needed to allow tables created after
 * the transaction was started but before the query was issued to be visible to the query.
 */
public class QueryCatalogVersions {
    private final int baseVersion;
    private final Map<Integer, Integer> tableOverrides;

    /**
     * Constructor.
     */
    public QueryCatalogVersions(int baseVersion, Map<Integer, Integer> tableOverrides) {
        this.baseVersion = baseVersion;
        this.tableOverrides = Map.copyOf(tableOverrides);
    }

    /**
     * Returns base catalog version (corresponding to the start timestamp of the transaction of this query).
     */
    public int baseVersion() {
        return baseVersion;
    }

    /**
     * Returns overrides for tables: a map from table IDs to catalog versions corresponding to the table ID.
     */
    public Map<Integer, Integer> tableOverrides() {
        return tableOverrides;
    }

    /**
     * Returns maximum of {@link #baseVersion()} and all versions in {@link #tableOverrides()}.
     */
    public int maxVersion() {
        int max = baseVersion;

        for (int overrideVersion : tableOverrides.values()) {
            if (overrideVersion > max) {
                max = overrideVersion;
            }
        }

        return max;
    }

    /**
     * Returns catalog version corresponding to the given table ID (it's either an override for this ID, or the base version).
     */
    public int catalogVersionForTable(int tableId) {
        Integer override = tableOverrides.get(tableId);

        return override != null ? override : baseVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueryCatalogVersions that = (QueryCatalogVersions) o;

        if (baseVersion != that.baseVersion) {
            return false;
        }
        return tableOverrides.equals(that.tableOverrides);
    }

    @Override
    public int hashCode() {
        int result = baseVersion;
        result = 31 * result + tableOverrides.hashCode();
        return result;
    }
}
