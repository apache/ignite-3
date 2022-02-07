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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.client.proto.query.ClientMessage;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC SQL query with parameters.
 */
public class Query implements ClientMessage {
    /** Query SQL. */
    private String sql;

    /** Arguments. */
    private List<Object[]> args;

    /**
     * Default constructor is used for serialization.
     */
    public Query() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param sql  Query SQL.
     */
    public Query(String sql) {
        this.sql = sql;
    }

    /**
     * Get the sql query.
     *
     * @return Query SQL string.
     */
    public String sql() {
        return sql;
    }

    /**
     * Get the sql arguments.
     *
     * @return Query arguments.
     */
    public List<Object[]> args() {
        return args;
    }

    /**
     * Adds batched arguments.
     *
     * @param args Batched arguments.
     */
    public void addBatchedArgs(Object[] args) {
        if (this.args == null) {
            this.args = new ArrayList<>();
        }

        this.args.add(args);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        ClientMessageUtils.writeStringNullable(packer, sql);
        if (args == null) {
            packer.packNil();
        } else {
            packer.packArrayHeader(args.size());
            for (Object[] arg : args) {
                packer.packObjectArray(arg);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        sql = ClientMessageUtils.readStringNullable(unpacker);
        if (unpacker.tryUnpackNil()) {
            args = null;
        } else {
            int size = unpacker.unpackArrayHeader();

            args = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                args.add(unpacker.unpackObjectArray());
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(Query.class, this);
    }
}
