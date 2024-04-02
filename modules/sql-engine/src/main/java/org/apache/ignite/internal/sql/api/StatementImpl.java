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

package org.apache.ignite.internal.sql.api;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.Statement;

/**
 * Statement.
 */
class StatementImpl implements Statement {
    /** Query. */
    private final String query;

    /**
     * Constructor.
     *
     * @param query Query.
     */
    public StatementImpl(String query) {
        this.query = query;
    }

    /** {@inheritDoc} */
    @Override
    public String query() {
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public long queryTimeout(TimeUnit timeUnit) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public int pageSize() {
        // TODO https://issues.apache.org/jira/browse/IGNITE-18647
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public ZoneId timeZoneId() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder toBuilder() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
