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

package org.apache.ignite.client.fakes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Session.SessionBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Client SQL session builder.
 */
public class FakeSessionBuilder implements SessionBuilder {
    private final Map<String, Object> properties = new HashMap<>();

    private final FakeIgniteSql sql;

    private String defaultSchema;

    private Long defaultQueryTimeoutMs;

    private Long defaultSessionTimeoutMs;

    private Integer pageSize;

    public FakeSessionBuilder(FakeIgniteSql sql) {
        this.sql = sql;
    }

    /** {@inheritDoc} */
    @Override
    public long defaultQueryTimeout(TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        return timeUnit.convert(defaultQueryTimeoutMs == null ? 0 : defaultQueryTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder defaultQueryTimeout(long timeout, TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        defaultQueryTimeoutMs = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);

        return this;
    }

    @Override
    public long idleTimeout(TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        return timeUnit.convert(defaultSessionTimeoutMs == null ? 0 : defaultSessionTimeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public SessionBuilder idleTimeout(long timeout, TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        defaultSessionTimeoutMs = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        return defaultSchema;
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder defaultSchema(String schema) {
        defaultSchema = schema;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public int defaultPageSize() {
        return pageSize == null ? 0 : pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder defaultPageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object property(String name) {
        return properties.get(name);
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder property(String name, @Nullable Object value) {
        properties.put(name, value);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public Session build() {
        return new FakeSession(pageSize, defaultSchema, defaultQueryTimeoutMs, defaultSessionTimeoutMs, new HashMap<>(properties), sql);
    }
}
