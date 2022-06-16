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

package org.apache.ignite.internal.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcRequestStatus;
import org.apache.ignite.internal.jdbc.proto.event.QueryFetchResult;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC query async result.
 */
public class JdbcClientQueryAsyncResult {
    /** Cursor ID for DML\DDL queries. */
    private static final long UNDEFINED_CURSOR = -1;

    /** Cursor ID. */
    private final long cursorId;

    /** Query result rows. */
    private List<List<Object>> rows;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /** Flag indicating the query is SELECT query. {@code false} for DML/DDL queries. */
    private final boolean isQuery;

    /** Update count. */
    private final long updateCnt;

    /** Client channel. */
    private final ClientChannel channel;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param in Unpacker.
     */
    public JdbcClientQueryAsyncResult(ClientChannel ch, ClientMessageUnpacker in) {
        this.channel = ch;

        if (in.tryUnpackNil()) {
            cursorId = UNDEFINED_CURSOR;
        } else {
            cursorId = in.unpackLong();
        }
        isQuery = in.unpackBoolean();
        updateCnt = in.unpackLong();
        last = in.unpackBoolean();

        int size = in.unpackArrayHeader();

        if (size == 0) {
            this.rows = Collections.emptyList();
        } else {
            rows = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                rows.add(Arrays.asList(in.unpackObjectArray()));
            }
        }
    }

    /**
     * Get the items.
     *
     * @return Query result rows.
     */
    public List<List<Object>> rows() {
        return rows;
    }

    /**
     * Get the last flag.
     *
     * @return Flag indicating the query has no unfetched results.
     */
    public boolean last() {
        return last;
    }

    /**
     * Get the isQuery flag.
     *
     * @return Flag indicating the query is SELECT query. {@code false} for DML/DDL queries.
     */
    public boolean isQuery() {
        return isQuery;
    }

    /**
     * Get the update count.
     *
     * @return Update count for DML queries.
     */
    public long updateCount() {
        return updateCnt;
    }

    /**
     * Fetch next batch.
     *
     * @param fetchSize Fetch size.
     */
    public void next(int fetchSize) throws SQLException {
        assert cursorId != UNDEFINED_CURSOR : "Fetch for DDL and DML queries is prohibited";

        CompletableFuture<QueryFetchResult> f = channel.serviceAsync(
                ClientOp.JDBC_NEXT, w -> {
                    w.out().packLong(cursorId);
                    w.out().packInt(fetchSize);
                }, p -> {
                    byte status = p.in().unpackByte();

                    if (status == JdbcRequestStatus.FAILED.getStatus()) {
                        throw new SQLException(p.in().unpackString(), SqlStateCode.INTERNAL_ERROR);
                    }

                    QueryFetchResult res = new QueryFetchResult();

                    res.readBinary(p.in());

                    return res;
                });

        QueryFetchResult queryFetchResult = getOrThrow(f);

        this.rows = queryFetchResult.items();
        this.last = queryFetchResult.last();
    }

    /**
     * Closes remote cursor.
     */
    public void close() throws SQLException {
        assert cursorId != UNDEFINED_CURSOR : "Close for DDL and DML queries is prohibited";

        CompletableFuture<Object> f = channel.serviceAsync(ClientOp.JDBC_CURSOR_CLOSE,
                w -> w.out().packLong(cursorId), p -> {
                    byte status = p.in().unpackByte();

                    if (status == JdbcRequestStatus.FAILED.getStatus()) {
                        throw new SQLException(p.in().unpackString(), SqlStateCode.INTERNAL_ERROR);
                    }

                    return null;
                });

        //Do nothing, result is just a marker of success operation.
        getOrThrow(f);
    }

    /**
     * Request result set metadata.
     *
     * @return result set metadata list.
     */
    public List<JdbcColumnMeta> metadata() throws SQLException {
        assert cursorId != UNDEFINED_CURSOR : "Metadata for DDL and DML queries is prohibited";

        CompletableFuture<JdbcMetaColumnsResult> f = channel.serviceAsync(
                ClientOp.JDBC_QUERY_META, w -> w.out().packLong(cursorId), p -> {
                    byte status = p.in().unpackByte();

                    if (status == JdbcRequestStatus.FAILED.getStatus()) {
                        throw new SQLException(p.in().unpackString(), SqlStateCode.INTERNAL_ERROR);
                    }

                    JdbcMetaColumnsResult res = new JdbcMetaColumnsResult();

                    res.readBinary(p.in());

                    return res;
                });

        return getOrThrow(f).meta();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcClientQueryAsyncResult.class, this);
    }

    private static <T> T getOrThrow(CompletableFuture<T> future) throws SQLException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new SQLException(e.getMessage(), SqlStateCode.INTERNAL_ERROR);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw new SQLException(e.getMessage(), SqlStateCode.INTERNAL_ERROR, e);
        }
    }
}
