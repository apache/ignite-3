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

    private String defaultSchema;

    private Long defaultTimeoutMs;

    private Integer pageSize;

    /** {@inheritDoc} */
    @Override
    public long defaultTimeout(TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        return timeUnit.convert(defaultTimeoutMs == null ? 0 : defaultTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder defaultTimeout(long timeout, TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        defaultTimeoutMs = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);

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
        return new FakeSession(pageSize, defaultSchema, defaultTimeoutMs, new HashMap<>(properties));
    }
}
