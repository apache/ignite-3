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

package org.apache.ignite.client.fakes;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;

/**
 * Fake {@link QueryProcessor}.
 */
public class FakeIgniteQueryProcessor implements QueryProcessor {
    @Override
    public List<CompletableFuture<AsyncSqlCursor<List<Object>>>> queryAsync(String schemaName, String qry, Object... params) {
        return List.of(CompletableFuture.completedFuture(new FakeCursor()));
    }

    @Override
    public List<CompletableFuture<AsyncSqlCursor<List<Object>>>> queryAsync(QueryContext context, String schemaName,
            String qry, Object... params) {
        return List.of(CompletableFuture.completedFuture(new FakeCursor()));
    }

    @Override
    public CompletableFuture<AsyncSqlCursor<List<Object>>> querySingleAsync(QueryContext context, String schemaName, String qry,
            Object... params) {
        return CompletableFuture.completedFuture(new FakeCursor());
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() throws Exception {

    }
}
