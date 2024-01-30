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

import static org.apache.ignite.internal.binarytuple.BinaryTupleParser.ORDER;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC query fetch result.
 */
public class JdbcQueryFetchResult extends Response {
    /** Serialized result rows. */
    private List<ByteBuffer> rowTuples;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcQueryFetchResult() {
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err    Error message.
     */
    public JdbcQueryFetchResult(int status, String err) {
        super(status, err);
    }

    /**
     * Constructor.
     *
     * @param rowTuples Serialized SQL result rows.
     * @param last  Flag indicating the query has no unfetched results.
     */
    public JdbcQueryFetchResult(List<ByteBuffer> rowTuples, boolean last) {
        Objects.requireNonNull(rowTuples);

        this.rowTuples = rowTuples;
        this.last = last;

        hasResults = true;
    }

    /**
     * Get the serialized result rows.
     *
     * @return Serialized query result rows.
     */
    public List<ByteBuffer> items() {
        return rowTuples;
    }

    /**
     * Get the last flag.
     *
     * @return Flag indicating the query has no unfetched results.
     */
    public boolean last() {
        return last;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!hasResults) {
            return;
        }

        packer.packBoolean(last);

        packer.packInt(rowTuples.size());

        for (ByteBuffer item : rowTuples) {
            packer.packByteBuffer(item);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults) {
            return;
        }

        last = unpacker.unpackBoolean();

        int size = unpacker.unpackInt();

        rowTuples = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            rowTuples.add(ByteBuffer.wrap(unpacker.readBinary()).order(ORDER));
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcQueryFetchResult.class, this);
    }
}
