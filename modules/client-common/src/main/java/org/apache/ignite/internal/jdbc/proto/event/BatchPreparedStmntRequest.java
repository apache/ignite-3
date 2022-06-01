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
public class BatchPreparedStmntRequest implements ClientMessage {
    /** Schema name. */
    private String schemaName;

    /** Sql query. */
    private String query;

    /** Batch of query arguments. */
    private List<Object[]> args;

    /**
     * Default constructor.
     */
    public BatchPreparedStmntRequest() {
    }

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param query Sql query string.
     * @param args Sql query arguments.
     */
    public BatchPreparedStmntRequest(String schemaName, String query, List<Object[]> args) {
        assert !StringUtil.isNullOrEmpty(query);
        assert !CollectionUtils.nullOrEmpty(args);

        this.query = query;
        this.args = args;
        this.schemaName = schemaName;
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

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        ClientMessageUtils.writeStringNullable(packer, schemaName);

        packer.packString(query);
        packer.packArrayHeader(args.size());

        for (Object[] arg : args) {
            packer.packObjectArray(arg);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        schemaName = ClientMessageUtils.readStringNullable(unpacker);

        query = unpacker.unpackString();

        int n = unpacker.unpackArrayHeader();

        args = new ArrayList<>(n);

        for (int i = 0; i < n; ++i) {
            args.add(unpacker.unpackObjectArray());
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(BatchPreparedStmntRequest.class, this);
    }
}
