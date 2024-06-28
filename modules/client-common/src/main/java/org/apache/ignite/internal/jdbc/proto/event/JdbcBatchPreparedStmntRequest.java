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

import io.netty.util.internal.StringUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.jdbc.proto.ClientMessage;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.CollectionUtils;

/**
 * JDBC prepared statement query batch execute request.
 */
public class JdbcBatchPreparedStmntRequest implements ClientMessage {
    /** Schema name. */
    private String schemaName;

    /** Sql query. */
    private String query;

    /** Batch of query arguments. */
    private List<Object[]> args;

    /** Flag indicating whether auto-commit mode is enabled. */
    private boolean autoCommit;

    /** Query timeout in milliseconds. */
    private long queryTimeoutMillis;

    /**
     * Default constructor.
     */
    public JdbcBatchPreparedStmntRequest() {
    }

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param query Sql query string.
     * @param args Sql query arguments.
     * @param autoCommit Flag indicating whether auto-commit mode is enabled.
     * @param queryTimeoutMillis Query timeout in millseconds.
     */
    public JdbcBatchPreparedStmntRequest(
            String schemaName, 
            String query, 
            List<Object[]> args, 
            boolean autoCommit, 
            long queryTimeoutMillis
    ) {
        assert !StringUtil.isNullOrEmpty(query);
        assert !CollectionUtils.nullOrEmpty(args);

        this.query = query;
        this.args = args;
        this.schemaName = schemaName;
        this.autoCommit = autoCommit;
        this.queryTimeoutMillis = queryTimeoutMillis;
    }

    /**
     * Get the schema name.
     *
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * Get the sql query string.
     *
     * @return Query string.
     */
    public String getQuery() {
        return query;
    }

    /**
     * Get the query arguments batch.
     *
     * @return query arguments batch.
     */
    public List<Object[]> getArgs() {
        return args;
    }

    /**
     * Get flag indicating whether auto-commit mode is enabled.
     *
     * @return {@code true} if auto-commit mode is enabled, {@code false} otherwise.
     */
    public boolean autoCommit() {
        return autoCommit;
    }

    /**
     * Returns the timeout in milliseconds.
     *
     * @return Timeout in milliseconds.
     */
    public long queryTimeoutMillis() {
        return queryTimeoutMillis;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        packer.packBoolean(autoCommit);
        ClientMessageUtils.writeStringNullable(packer, schemaName);

        packer.packString(query);
        packer.packInt(args.size());

        for (Object[] arg : args) {
            packer.packObjectArrayAsBinaryTuple(arg, null);
        }

        packer.packLong(queryTimeoutMillis);
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        autoCommit = unpacker.unpackBoolean();
        schemaName = ClientMessageUtils.readStringNullable(unpacker);

        query = unpacker.unpackString();

        int n = unpacker.unpackInt();

        args = new ArrayList<>(n);

        for (int i = 0; i < n; ++i) {
            args.add(unpacker.unpackObjectArrayFromBinaryTuple());
        }

        queryTimeoutMillis = unpacker.unpackLong();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcBatchPreparedStmntRequest.class, this);
    }
}
