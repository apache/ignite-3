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

import java.util.concurrent.CompletionStage;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

public class FakeAsyncResultSet implements AsyncResultSet {
    private final Session session;
    private final Transaction transaction;
    private final Statement statement;
    private final Object[] arguments;

    public FakeAsyncResultSet(Session session, Transaction transaction, Statement statement, Object[] arguments) {
        this.session = session;
        this.transaction = transaction;
        this.statement = statement;
        this.arguments = arguments;
    }

    @Override
    public @Nullable ResultSetMetadata metadata() {
        return null;
    }

    @Override
    public boolean hasRowSet() {
        return false;
    }

    @Override
    public long affectedRows() {
        return 0;
    }

    @Override
    public boolean wasApplied() {
        return false;
    }

    @Override
    public Iterable<SqlRow> currentPage() {
        return null;
    }

    @Override
    public int currentPageSize() {
        return 0;
    }

    @Override
    public CompletionStage<? extends AsyncResultSet> fetchNextPage() {
        return null;
    }

    @Override
    public boolean hasMorePages() {
        return false;
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        return null;
    }
}
