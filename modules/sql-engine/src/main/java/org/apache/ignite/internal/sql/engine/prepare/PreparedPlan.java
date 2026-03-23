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

import java.time.Instant;

/**
 * Prepared plan.
 */
public final class PreparedPlan {

    private final String defaultSchemaName;

    private final int catalogVersion;

    private final QueryPlan queryPlan;

    private final String sql;

    private final Instant timestamp;

    /**
     * Constructor.
     */
    public PreparedPlan(CacheKey cacheKey, QueryPlan plan, Instant timestamp) {
        this.defaultSchemaName = cacheKey.schemaName();
        this.catalogVersion = cacheKey.catalogVersion();
        this.sql = cacheKey.query();
        this.queryPlan = plan;
        this.timestamp = timestamp;
    }

    /**
     * Catalog version.
     */
    public int catalogVersion() {
        return catalogVersion;
    }

    /**
     * Default schema.
     */
    public String defaultSchemaName() {
        return defaultSchemaName;
    }

    /**
     * Normalised SQL-string.
     */
    public String sql() {
        return sql;
    }

    /**
     * Query plan.
     */
    public QueryPlan queryPlan() {
        return queryPlan;
    }

    /**
     * A point in time when the plan was prepared.
     */
    public Instant timestamp() {
        return timestamp;
    }
}
