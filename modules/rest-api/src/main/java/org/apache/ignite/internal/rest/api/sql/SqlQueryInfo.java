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

package org.apache.ignite.internal.rest.api.sql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.RequiredMode;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Rest representation of sql query.
 */
@Schema(name = "SqlQuery")
public class SqlQueryInfo {

    /**
     * Sql Query ID.
     */
    @Schema(description = "Sql query ID.", requiredMode = RequiredMode.REQUIRED)
    private final UUID id;

    /**
     * Phase.
     */
    @Schema(description = "Phase.", requiredMode = RequiredMode.REQUIRED)
    private final String phase;


    /**
     * Type.
     */
    @Schema(description = "Type.", requiredMode = RequiredMode.REQUIRED)
    private final String type;

    /**
     * Schema.
     */
    @Schema(description = "Schema.", requiredMode = RequiredMode.REQUIRED)
    private final String schema;

    /**
     * SQL statement.
     */
    @Schema(description = "SQL statement.", requiredMode = RequiredMode.REQUIRED)
    private final String sql;

    /**
     * Start time.
     */
    @Schema(description = "Start time.", requiredMode = RequiredMode.REQUIRED)
    private final Instant startTime;


    /**
     * Constructor.
     *
     * @param id query id.
     * @param phase query phase.
     * @param type query type.
     * @param schema schema.
     * @param sql sql statement.
     * @param startTime query start time.
     */
    @JsonCreator
    public SqlQueryInfo(
            @JsonProperty("id") UUID id,
            @JsonProperty("phase") String phase,
            @JsonProperty("type") @Nullable String type,
            @JsonProperty("schema") String schema,
            @JsonProperty("sql") String sql,
            @JsonProperty("startTime") Instant startTime) {
        this.id = id;
        this.phase = phase;
        this.type = type;
        this.schema = schema;
        this.sql = sql;
        this.startTime = startTime;
    }

    @JsonProperty("id")
    public UUID id() {
        return id;
    }

    @JsonProperty("phase")
    public String phase() {
        return phase;
    }

    @JsonProperty("type")
    public String type() {
        return type;
    }

    @JsonProperty("schema")
    public String schema() {
        return schema;
    }

    @JsonProperty("sql")
    public String sql() {
        return sql;
    }

    @JsonProperty("startTime")
    public Instant startTime() {
        return startTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlQueryInfo queryInfo = (SqlQueryInfo) o;
        return Objects.equals(id, queryInfo.id) && Objects.equals(phase, queryInfo.phase) && Objects.equals(type,
                queryInfo.type) && Objects.equals(schema, queryInfo.schema) && Objects.equals(sql, queryInfo.sql)
                && Objects.equals(startTime, queryInfo.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, phase, type, schema, sql, startTime);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
