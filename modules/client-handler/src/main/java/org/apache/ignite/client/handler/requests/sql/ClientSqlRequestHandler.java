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

package org.apache.ignite.client.handler.requests.sql;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.client.proto.query.QueryEventHandler;
import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchResult;

/**
 * Client sql request handler.
 */
public class ClientSqlRequestHandler {
    /** Processor. */
    private final QueryEventHandler processor;

    /**
     * @param processor Processor.
     */
    public ClientSqlRequestHandler(QueryEventHandler processor) {
        this.processor = processor;
    }

    /**
     * Processes remote {@code JdbcQueryExecuteRequest}.
     *
     * @param in Client message unpacker.
     * @param out Client message packer.
     * @return null value indicates synchronous operation.
     * @throws IOException in case of network troubles.
     */
    public CompletableFuture<Void> execute(
            ClientMessageUnpacker in,
            ClientMessagePacker out
    ) throws IOException {
        var req = new JdbcQueryExecuteRequest();

        req.readBinary(in);

        JdbcQueryExecuteResult res = processor.query(req);

        res.writeBinary(out);

        return null;
    }

    /**
     * Processes remote {@code JdbcBatchExecuteRequest}.
     *
     * @param in Client message unpacker.
     * @param out Client message packer.
     * @return null value indicates synchronous operation.
     * @throws IOException in case of network troubles.
     */
    public CompletableFuture<Void> executeBatch(
        ClientMessageUnpacker in,
        ClientMessagePacker out
    ) throws IOException {
        var req = new JdbcBatchExecuteRequest();

        req.readBinary(in);

        JdbcBatchExecuteResult res = processor.batch(req);

        res.writeBinary(out);

        return null;
    }

    /**
     * Processes remote {@code JdbcQueryFetchRequest}.
     *
     * @param in Client message unpacker.
     * @param out Client message packer.
     * @return null value indicates synchronous operation.
     * @throws IOException in case of network troubles.
     */
    public CompletableFuture<Void> next(
        ClientMessageUnpacker in,
        ClientMessagePacker out
    ) throws IOException {
        var req = new JdbcQueryFetchRequest();

        req.readBinary(in);

        JdbcQueryFetchResult res = processor.fetch(req);

        res.writeBinary(out);

        return null;
    }

    /**
     * Processes remote {@code JdbcQueryCloseRequest}.
     *
     * @param in Client message unpacker.
     * @param out Client message packer.
     * @return null value indicates synchronous operation.
     * @throws IOException in case of network troubles.
     */
    public CompletableFuture<Void> close(
        ClientMessageUnpacker in,
        ClientMessagePacker out
    ) throws IOException {
        var req = new JdbcQueryCloseRequest();

        req.readBinary(in);

        JdbcQueryCloseResult res = processor.close(req);

        res.writeBinary(out);

        return null;
    }
}
