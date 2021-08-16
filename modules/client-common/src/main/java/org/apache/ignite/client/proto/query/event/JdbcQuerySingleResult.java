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

package org.apache.ignite.client.proto.query.event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC query execute result.
 */
public class JdbcQuerySingleResult extends JdbcResponse {
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

    /**
     * Constructor. For deserialization purposes only.
     */
    public JdbcQuerySingleResult() {
    }

    /**
     * Constructor.
     */
    public JdbcQuerySingleResult(int status, String err) {
        super(status, err);
    }

    /**
     * @param cursorId Cursor ID.
     * @param items Query result rows.
     * @param last Flag indicates the query has no unfetched results.
     */
    public JdbcQuerySingleResult(long cursorId, List<List<Object>> items, boolean last) {
        super();

        this.cursorId = cursorId;
        this.items = items;
        this.last = last;
        isQuery = true;
    }

    /**
     * @param cursorId Cursor ID.
     * @param updateCnt Update count for DML queries.
     */
    public JdbcQuerySingleResult(long cursorId, long updateCnt) {
        super();

        this.cursorId = cursorId;
        last = true;
        isQuery = false;
        this.updateCnt = updateCnt;
    }

    /**
     * @return Cursor ID.
     */
    public long cursorId() {
        return cursorId;
    }

    /**
     * @return Query result rows.
     */
    public List<List<Object>> items() {
        return items;
    }

    /**
     * @return Flag indicating the query has no unfetched results.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return Flag indicating the query is SELECT query. {@code false} for DML/DDL queries.
     */
    public boolean isQuery() {
        return isQuery;
    }

    /**
     * @return Update count for DML queries.
     */
    public long updateCount() {
        return updateCnt;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) throws IOException {
        super.writeBinary(packer);

        if (status() != STATUS_SUCCESS)
            return;

        packer.packLong(cursorId);
        packer.packBoolean(isQuery);

        if (isQuery) {
            assert items != null;

            packer.packBoolean(last);

            packer.packInt(items.size());

            for (List<Object> item : items)
                packer.packObjectArray(item.toArray());
        }
        else
            packer.packLong(updateCnt);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) throws IOException {
        super.readBinary(unpacker);

        if (status() != STATUS_SUCCESS)
            return;

        cursorId = unpacker.unpackLong();
        isQuery = unpacker.unpackBoolean();

        if (isQuery) {
            last = unpacker.unpackBoolean();

            int size = unpacker.unpackInt();

            if (size > 0) {
                items = new ArrayList<>(size);

                for (int i = 0; i < size; i++)
                    items.add(Arrays.asList(unpacker.unpackObjectArray()));
            }
        }
        else {
            last = true;

            updateCnt = unpacker.unpackLong();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQuerySingleResult.class, this);
    }
}
