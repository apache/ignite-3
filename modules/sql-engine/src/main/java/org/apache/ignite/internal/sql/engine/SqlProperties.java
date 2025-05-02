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

import static org.apache.ignite.internal.sql.engine.SqlQueryProcessor.DEFAULT_TIME_ZONE_ID;

import java.time.ZoneId;
import java.util.Set;
import org.apache.ignite.internal.sql.SqlCommon;

/**
 * An object that keeps values of the properties.
 */
public class SqlProperties {
    private long queryTimeout;
    private Set<SqlQueryType> allowedQueryTypes = SqlQueryType.ALL;
    private String defaultSchema = SqlCommon.DEFAULT_SCHEMA_NAME;
    private ZoneId timeZoneId = DEFAULT_TIME_ZONE_ID;

    public SqlProperties() {
    }

    /** Copy constructor. */
    public SqlProperties(SqlProperties other) {
        queryTimeout = other.queryTimeout;
        allowedQueryTypes = other.allowedQueryTypes;
        defaultSchema = other.defaultSchema;
        timeZoneId = other.timeZoneId;
    }

    public SqlProperties queryTimeout(long queryTimeout) {
        this.queryTimeout = queryTimeout;
        return this;
    }

    public long queryTimeout() {
        return queryTimeout;
    }

    public SqlProperties allowedQueryTypes(Set<SqlQueryType> allowedQueryTypes) {
        this.allowedQueryTypes = allowedQueryTypes;
        return this;
    }

    public Set<SqlQueryType> allowedQueryTypes() {
        return allowedQueryTypes;
    }

    public SqlProperties defaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
        return this;
    }

    public String defaultSchema() {
        return defaultSchema;
    }

    public SqlProperties timeZoneId(ZoneId timeZoneId) {
        this.timeZoneId = timeZoneId;
        return this;
    }

    public ZoneId timeZoneId() {
        return timeZoneId;
    }
}
