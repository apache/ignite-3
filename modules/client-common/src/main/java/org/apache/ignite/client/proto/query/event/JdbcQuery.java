/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client.proto.query.event;

import java.io.IOException;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC SQL query with parameters.
 */
public class JdbcQuery implements JdbcClientMessage {
    /** Query SQL. */
    private String sql;

    /** Arguments. */
    private Object[] args;

    /**
     * Default constructor is used for serialization.
     */
    public JdbcQuery() {
        // No-op.
    }

    /**
     * @param sql Query SQL.
     * @param args Arguments.
     */
    public JdbcQuery(String sql, Object[] args) {
        this.sql = sql;
        this.args = args;
    }

    /**
     * @return Query SQL string.
     */
    public String sql() {
        return sql;
    }

    /**
     * @return Query arguments.
     */
    public Object[] args() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) throws IOException {
        packer.packString(sql);
        packer.packObjectArray(args);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) throws IOException {
        sql = unpacker.unpackString();
        args = unpacker.unpackObjectArray();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQuery.class, this);
    }
}
