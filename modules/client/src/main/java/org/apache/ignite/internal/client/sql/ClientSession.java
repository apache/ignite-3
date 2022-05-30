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

package org.apache.ignite.internal.client.sql;

import static org.apache.ignite.internal.client.ClientUtils.sync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.sql.reactive.ReactiveResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client SQL session.
 */
public class ClientSession implements Session {
    /** Channel. */
    private final ReliableChannel ch;

    /**
     * Constructor.
     *
     * @param ch Channel.
     */
    public ClientSession(ReliableChannel ch) {
        this.ch = ch;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet execute(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        // TODO: Wrap AsyncResultSet.
        // return sync(executeAsync(transaction, query, arguments));
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet execute(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, Statement statement,
            @Nullable Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public ReactiveResultSet executeReactive(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public int[] executeBatch(@Nullable Transaction transaction, String dmlQuery, BatchedArguments batch) {
        return new int[0];
    }

    /** {@inheritDoc} */
    @Override
    public int[] executeBatch(@Nullable Transaction transaction, Statement dmlStatement, BatchedArguments batch) {
        return new int[0];
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<int[]> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<int[]> executeBatchAsync(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Integer> executeBatchReactive(@Nullable Transaction transaction, String query, BatchedArguments batch) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Integer> executeBatchReactive(@Nullable Transaction transaction, Statement statement, BatchedArguments batch) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void executeScript(String query, @Nullable Object... arguments) {

    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public long defaultTimeout(TimeUnit timeUnit) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public int defaultPageSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object property(String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        sync(closeAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        // TODO: Cancel/close all active futures.
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<Void> closeReactive() {
        // TODO: Future to Publisher.
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder toBuilder() {
        return null;
    }
}
