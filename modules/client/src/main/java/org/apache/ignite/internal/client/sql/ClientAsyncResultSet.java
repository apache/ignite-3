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

import java.util.concurrent.CompletionStage;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.jetbrains.annotations.Nullable;

/**
 * Client async result set.
 */
class ClientAsyncResultSet implements AsyncResultSet {
    @Override
    public @Nullable ResultSetMetadata metadata() {
        // TODO: IGNITE-17052
        throw new UnsupportedOperationException("Not implemented yet.");
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
