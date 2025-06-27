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

package org.apache.ignite.internal.jdbc;

import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQuerySingleResult;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC query execute result.
 */
public class JdbcQueryExecuteResponse extends Response {
    /** Query result. */
    private JdbcQuerySingleResult result;

    /** Client channel. */
    private final ClientChannel channel;

    /**
     * Constructor.
     *
     * @param channel Client channel.
     */
    public JdbcQueryExecuteResponse(ClientChannel channel) {
        this.channel = channel;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        result = new JdbcQuerySingleResult();

        result.readBinary(unpacker);
    }

    /** {@inheritDoc} */
    @Override
    public int status() {
        return result.status();
    }

    /** {@inheritDoc} */
    @Override
    public String err() {
        return result.err();
    }

    /** {@inheritDoc} */
    @Override
    public boolean success() {
        return result.success();
    }

    /**
     * Get the query results.
     *
     * @return Query result.
     */
    public JdbcQuerySingleResult result() {
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcQueryExecuteResponse.class, this);
    }

    /** Gets client channel. */
    public ClientChannel getChannel() {
        return channel;
    }
}
