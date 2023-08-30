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

package org.apache.ignite.internal.client.sql;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Session.SessionBuilder;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;

/**
 * Client SQL session builder.
 */
public class ClientSessionBuilder implements SessionBuilder {
    /** Channel. */
    private final ReliableChannel ch;

    /** Properties. */
    private final Map<String, Object> properties = new HashMap<>();

    /** Default schema. */
    private String defaultSchema;

    /** Default query timeout. */
    private Long defaultQueryTimeoutMs;

    /** Default session timeout. */
    private Long defaultSessionTimeoutMs;

    /** Page size. */
    private Integer pageSize;

    /**
     * Constructor.
     *
     * @param ch Channel.
     */
    public ClientSessionBuilder(ReliableChannel ch) {
        this.ch = ch;
    }

    @Override
    public IgniteTransactions igniteTransactions() {
        return null;
    }

    @Override
    public SessionBuilder igniteTransactions(IgniteTransactions transactions) {
        return this;
    }

    @Override
    public long defaultQueryTimeout(TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        return timeUnit.convert(defaultQueryTimeoutMs == null ? 0 : defaultQueryTimeoutMs, TimeUnit.MILLISECONDS);
    }

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

        defaultSessionTimeoutMs = timeUnit.toMillis(timeout);

        return this;
    }

    @Override
    public String defaultSchema() {
        return defaultSchema;
    }

    @Override
    public SessionBuilder defaultSchema(String schema) {
        defaultSchema = schema;

        return this;
    }

    @Override
    public int defaultPageSize() {
        return pageSize == null ? 0 : pageSize;
    }

    @Override
    public SessionBuilder defaultPageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    @Override
    public @Nullable Object property(String name) {
        return properties.get(name);
    }

    @Override
    public SessionBuilder property(String name, @Nullable Object value) {
        properties.put(name, value);

        return this;
    }

    @Override
    public Session build() {
        return new ClientSession(ch, pageSize, defaultSchema, defaultQueryTimeoutMs, defaultSessionTimeoutMs, new HashMap<>(properties));
    }
}
