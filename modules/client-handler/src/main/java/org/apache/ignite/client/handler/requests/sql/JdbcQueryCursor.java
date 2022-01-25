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

package org.apache.ignite.client.handler.requests.sql;

import java.util.Iterator;
import org.apache.ignite.internal.sql.engine.ResultSetMetadata;
import org.apache.ignite.internal.sql.engine.SqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;

/**
 * Sql query cursor with jdbc related parameters.
 */
public class JdbcQueryCursor<T> implements SqlCursor<T> {
    /** Max rows. */
    private final long maxRows;

    /** Query result rows. */
    private final SqlCursor<T> cur;

    /** Query results iterator. */
    private final Iterator<T> iter;

    /** Number of fetched rows. */
    private long fetched;

    /**
     * Constructor.
     *
     * @param maxRows Max rows.
     * @param cur Query cursor.
     */
    public JdbcQueryCursor(int maxRows, SqlCursor<T> cur) {
        this.maxRows = maxRows;
        this.cur = cur;
        this.iter = cur.iterator();

        this.fetched = 0;
    }

    /**
     * Returns true if the iteration has more elements and the limit of maxRows has not been reached.
     *
     * @return {@code true} if the cursor has more rows and the limit of the maximum rows hasn't been reached.
     */
    @Override
    public boolean hasNext() {
        return iter.hasNext() && !(maxRows > 0 && fetched >= maxRows);
    }

    /**
     * Returns the next element in the iteration if the iteration has more elements
     * and the limit of maxRows has not been reached.
     *
     * @return the next element if the cursor has more rows and the limit of the maximum rows hasn't been reached.
     */
    @Override
    public T next() {
        int fetchSize = (maxRows > 0) ? (int) Math.min(1, maxRows - fetched) : 1;

        if (fetchSize > 0) {
            fetched++;

            return iter.next();
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        cur.close();
    }

    /** {@inheritDoc} */
    @Override
    public SqlQueryType queryType() {
        return cur.queryType();
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return cur.metadata();
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<T> iterator() {
        return cur.iterator();
    }
}
