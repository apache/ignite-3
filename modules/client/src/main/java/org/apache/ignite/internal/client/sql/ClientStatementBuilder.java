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

package org.apache.ignite.internal.client.sql;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class ClientStatementBuilder implements Statement.StatementBuilder {
    private String query;

    private String defaultSchema;

    private boolean prepared;

    private long queryTimeoutMs;

    private int pageSize;

    @Override
    public @NotNull String query() {
        return query;
    }

    @Override
    public StatementBuilder query(String sql) {
        query = sql;

        return this;
    }

    @Override
    public boolean prepared() {
        return prepared;
    }

    @Override
    public StatementBuilder prepared(boolean prepared) {
        this.prepared = prepared;

        return this;
    }

    @Override
    public long queryTimeout(@NotNull TimeUnit timeUnit) {
        return timeUnit.convert(queryTimeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public StatementBuilder queryTimeout(long timeout, @NotNull TimeUnit timeUnit) {
        queryTimeoutMs = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);

        return this;
    }

    @Override
    public String defaultSchema() {
        return defaultSchema;
    }

    @Override
    public StatementBuilder defaultSchema(@NotNull String schema) {
        defaultSchema = schema;

        return this;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public StatementBuilder pageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    @Override
    public @Nullable Object property(@NotNull String name) {
        return null;
    }

    @Override
    public StatementBuilder property(@NotNull String name, @Nullable Object value) {
        return null;
    }

    @Override
    public Statement build() {
        return null;
    }
}
