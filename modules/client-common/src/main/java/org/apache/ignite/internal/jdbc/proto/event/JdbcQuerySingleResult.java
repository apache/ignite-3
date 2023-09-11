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

package org.apache.ignite.internal.jdbc.proto.event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC query execute result.
 */
public class JdbcQuerySingleResult extends Response {
    /** Cursor ID. */
    private Long cursorId;

    /** Query result rows. */
    private List<List<Object>> items;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /** Flag indicating the query is SELECT query. {@code false} for DML/DDL queries. */
    private boolean isQuery;

    /** Update count. */
    private long updateCnt;

    /**
     * Constructor. For deserialization purposes only.
     */
    public JdbcQuerySingleResult() {
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err    Error message.
     */
    public JdbcQuerySingleResult(int status, String err) {
        super(status, err);
    }

    /**
     * Constructor.
     *
     * @param cursorId Cursor ID.
     * @param items    Query result rows.
     * @param last     Flag indicates the query has no unfetched results.
     */
    public JdbcQuerySingleResult(long cursorId, List<List<Object>> items, boolean last) {
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
     * @param updateCnt Update count for DML queries.
     */
    public JdbcQuerySingleResult(long updateCnt) {
        super();

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
    public Long cursorId() {
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

        if (cursorId != null) {
            packer.packLong(cursorId);
        } else {
            packer.packNil();
        }

        packer.packBoolean(isQuery);
        packer.packLong(updateCnt);
        packer.packBoolean(last);

        packer.packInt(items.size());

        for (List<Object> item : items) {
            packer.packObjectArrayAsBinaryTuple(item.toArray());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults) {
            return;
        }

        if (unpacker.tryUnpackNil()) {
            cursorId = null;
        } else {
            cursorId = unpacker.unpackLong();
        }
        isQuery = unpacker.unpackBoolean();
        updateCnt = unpacker.unpackLong();
        last = unpacker.unpackBoolean();

        int size = unpacker.unpackInt();

        items = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            items.add(Arrays.asList(unpacker.unpackObjectArrayFromBinaryTuple()));
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcQuerySingleResult.class, this);
    }
}
