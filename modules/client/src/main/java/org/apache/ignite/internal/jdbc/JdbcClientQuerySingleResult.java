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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.QueryCloseResult;
import org.apache.ignite.internal.jdbc.proto.event.QueryFetchResult;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC query execute result.
 */
public class JdbcClientQuerySingleResult extends Response {
    /** Cursor ID. */
    private long cursorId;

    /** Query result rows. */
    private List<List<Object>> items;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /** Flag indicating the query is SELECT query. {@code false} for DML/DDL queries. */
    private boolean isQuery;

    /** Update count. */
    private long updateCnt;
    private ClientChannel channel;

    /**
     * Constructor. For deserialization purposes only.
     */
    public JdbcClientQuerySingleResult(ClientChannel channel) {
        this.channel = channel;
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err    Error message.
     */
    public JdbcClientQuerySingleResult(int status, String err) {
        super(status, err);
    }

    /**
     * Constructor.
     *
     * @param cursorId Cursor ID.
     * @param items    Query result rows.
     * @param last     Flag indicates the query has no unfetched results.
     */
    public JdbcClientQuerySingleResult(long cursorId, List<List<Object>> items, boolean last) {
        super();

        Objects.requireNonNull(items);

        this.cursorId = cursorId;
        this.items = items;
        this.last = last;
        this.isQuery = true;

        hasResults = true;
    }

    /**
     * Constructor.
     *
     * @param cursorId  Cursor ID.
     * @param updateCnt Update count for DML queries.
     */
    public JdbcClientQuerySingleResult(long cursorId, long updateCnt) {
        super();

        this.cursorId = cursorId;
        this.last = true;
        this.isQuery = false;
        this.updateCnt = updateCnt;
        this.items = Collections.emptyList();

        hasResults = true;
    }

    /**
     * Get the cursor id.
     *
     * @return Cursor ID.
     */
    public long cursorId() {
        return cursorId;
    }

    /**
     * Get the items.
     *
     * @return Query result rows.
     */
    public List<List<Object>> items() {
        return items;
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

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!hasResults) {
            return;
        }

        packer.packLong(cursorId);
        packer.packBoolean(isQuery);
        packer.packLong(updateCnt);
        packer.packBoolean(last);

        packer.packArrayHeader(items.size());

        for (List<Object> item : items) {
            packer.packObjectArray(item.toArray());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults) {
            return;
        }

        cursorId = unpacker.unpackLong();
        isQuery = unpacker.unpackBoolean();
        updateCnt = unpacker.unpackLong();
        last = unpacker.unpackBoolean();

        int size = unpacker.unpackArrayHeader();

        items = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            items.add(Arrays.asList(unpacker.unpackObjectArray()));
        }
    }

    public CompletableFuture<Object> nextAsync(int fetchSize) {
        return channel.serviceAsync(ClientOp.JDBC_NEXT,  w -> {
            w.out().packLong(cursorId);
            w.out().packInt(fetchSize);
        }, p -> {
            QueryFetchResult res = new QueryFetchResult();

            res.readBinary(p.in());

            return res;
        }).thenApply(res -> {
            this.items = res.items();
            this.last = res.last();

            return true;
        });
    }

    public QueryCloseResult closeAsync() {
        return channel.serviceAsync(ClientOp.JDBC_CURSOR_CLOSE,  w -> w.out().packLong(cursorId), p -> {
            QueryCloseResult res = new QueryCloseResult();

            res.readBinary(p.in());

            return res;
        }).join();
    }

    public JdbcMetaColumnsResult metadataAsync() {
        return channel.serviceAsync(ClientOp.JDBC_QUERY_META,  w -> w.out().packLong(cursorId), p -> {
            JdbcMetaColumnsResult res = new JdbcMetaColumnsResult();

            res.readBinary(p.in());

            return res;
        }).join();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcClientQuerySingleResult.class, this);
    }
}
