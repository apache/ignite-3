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

package org.apache.ignite.internal.sql.api;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Session.SessionBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Session builder implementation.
 */
public class SessionBuilderImpl implements SessionBuilder {
    public static final int DEFAULT_PAGE_SIZE = 1024;

    public static final long DEFAULT_TIMEOUT = 0;

    private final QueryProcessor qryProc;

    private long timeout = DEFAULT_TIMEOUT;

    private String schema;

    private int pageSize = DEFAULT_PAGE_SIZE;

    SessionBuilderImpl(QueryProcessor qryProc) {
        this.qryProc = qryProc;
    }

    /** {@inheritDoc} */
    @Override
    public long defaultTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(timeout, TimeUnit.NANOSECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder defaultTimeout(long timeout, TimeUnit timeUnit) {
        this.timeout = timeUnit.toNanos(timeout);

        return this;
    }

    @Override
    public String defaultSchema() {
        return schema;
    }

    @Override
    public SessionBuilder defaultSchema(String schema) {
        this.schema = schema;

        return this;
    }

    @Override
    public int defaultPageSize() {
        return pageSize;
    }

    @Override
    public SessionBuilder defaultPageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    @Override
    public @Nullable Object property(String name) {
        return null;
    }

    @Override
    public SessionBuilder property(String name, @Nullable Object value) {
        return null;
    }

    @Override
    public Session build() {
        return new SessionImpl(
                qryProc,
                schema,
                timeout,
                pageSize
        );
    }
}
