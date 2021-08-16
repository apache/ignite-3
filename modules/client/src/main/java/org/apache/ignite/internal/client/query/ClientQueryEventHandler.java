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

package org.apache.ignite.internal.client.query;

import org.apache.ignite.client.proto.ClientOp;
import org.apache.ignite.client.proto.query.QueryEventHandler;
import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchResult;
import org.apache.ignite.internal.client.ReliableChannel;

/**
 *  Jdbc query network event handler implementation.
 */
public class ClientQueryEventHandler implements QueryEventHandler {
    /** Channel. */
    private final ReliableChannel ch;

    /**
     * @param ch Channel.
     */
    public ClientQueryEventHandler(ReliableChannel ch) {
        this.ch = ch;
    }

    /** {@inheritDoc} */
    @Override public JdbcQueryExecuteResult query(JdbcQueryExecuteRequest req) {
        return ch.serviceAsync(ClientOp.SQL_EXEC, w -> req.writeBinary(w.out()), p -> {
            JdbcQueryExecuteResult res = new JdbcQueryExecuteResult();
            res.readBinary(p.in());

            return res;
        }).join();
    }

    /** {@inheritDoc} */
    @Override public JdbcQueryFetchResult fetch(JdbcQueryFetchRequest req) {
        return ch.serviceAsync(ClientOp.SQL_NEXT, w -> req.writeBinary(w.out()), p -> {
            JdbcQueryFetchResult res = new JdbcQueryFetchResult();
            res.readBinary(p.in());

            return res;
        }).join();
    }

    /** {@inheritDoc} */
    @Override public JdbcBatchExecuteResult batch(JdbcBatchExecuteRequest req) {
        return ch.serviceAsync(ClientOp.SQL_EXEC_BATCH, w -> req.writeBinary(w.out()), p -> {
            JdbcBatchExecuteResult res = new JdbcBatchExecuteResult();
            res.readBinary(p.in());

            return res;
        }).join();
    }

    /** {@inheritDoc} */
    @Override public JdbcQueryCloseResult close(JdbcQueryCloseRequest req) {
        return ch.serviceAsync(ClientOp.SQL_CURSOR_CLOSE, w -> req.writeBinary(w.out()), p -> {
            JdbcQueryCloseResult res = new JdbcQueryCloseResult();
            res.readBinary(p.in());

            return res;
        }).join();
    }
}
