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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryCursorHandler;
import org.apache.ignite.internal.jdbc.proto.event.JdbcFetchQueryResultsRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryFetchResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryMetadataRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQuerySingleResult;

/**
 * Jdbc client cursor events handler implementation.
 */
public class JdbcClientQueryCursorHandler implements JdbcQueryCursorHandler {
    /** Channel. */
    private final ClientChannel channel;

    /**
     * Constructor.
     *
     * @param channel Client channel.
     */
    JdbcClientQueryCursorHandler(ClientChannel channel) {
        this.channel = channel;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQueryFetchResult> fetchAsync(JdbcFetchQueryResultsRequest req) {
        return channel.serviceAsync(ClientOp.JDBC_NEXT, w -> req.writeBinary(w.out()), r -> {
            JdbcQueryFetchResult res = new JdbcQueryFetchResult();

            res.readBinary(r.in());

            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQuerySingleResult> getMoreResultsAsync(JdbcFetchQueryResultsRequest req) {
        return channel.serviceAsync(ClientOp.JDBC_MORE_RESULTS, w -> req.writeBinary(w.out()), r -> {
            JdbcQuerySingleResult res = new JdbcQuerySingleResult();

            res.readBinary(r.in());

            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQueryCloseResult> closeAsync(JdbcQueryCloseRequest req) {
        return channel.serviceAsync(ClientOp.JDBC_CURSOR_CLOSE, w -> req.writeBinary(w.out()), r -> {
            JdbcQueryCloseResult res = new JdbcQueryCloseResult();

            res.readBinary(r.in());

            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaColumnsResult> queryMetadataAsync(JdbcQueryMetadataRequest req) {
        return channel.serviceAsync(ClientOp.JDBC_QUERY_META, w -> req.writeBinary(w.out()), r -> {
            JdbcMetaColumnsResult res = new JdbcMetaColumnsResult();

            res.readBinary(r.in());

            return res;
        });
    }
}
