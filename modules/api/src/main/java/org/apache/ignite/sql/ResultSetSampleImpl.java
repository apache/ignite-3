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

package org.apache.ignite.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * SAMPLE IMPLEMENTATION to demonstrate sync/async working together.
 */
public class ResultSetSampleImpl implements ResultSet {
    private List<SqlRow> page = new ArrayList<>();

    private boolean iteratorStarted;

    private boolean hasMorePages;

    @Override
    public @Nullable ResultSetMetadata metadata() {
        return null;
    }

    @Override
    public boolean hasRowSet() {
        return true;
    }

    @Override
    public int affectedRows() {
        return 1000;
    }

    @Override
    public boolean wasApplied() {
        return false;
    }

    @Override
    public Iterable<SqlRow> currentPage() {
        if (iteratorStarted) {
            throw new IgniteException("currentPage can't be used together with iterator.");
        }

        return page;
    }

    @Override
    public CompletionStage<? extends ResultSet> fetchNextPage() {
        if (iteratorStarted) {
            throw new IgniteException("fetchNextPage can't be used together with iterator.");
        }

        return fetchNextPageInternal().thenApply(p -> this);
    }

    @Override
    public boolean hasMorePages() {
        return true;
    }

    @Override
    public void close() throws Exception {

    }

    private CompletableFuture<Void> fetchNextPageInternal() {
        return CompletableFuture.<List<SqlRow>>completedFuture(new ArrayList<>()).thenAccept(p -> {
            // Data below comes from the SQL engine.
            page = p;
            hasMorePages = true;
        });
    }

    @NotNull
    @Override
    public Iterator<SqlRow> iterator() {
        if (iteratorStarted) {
            throw new IgniteException("Iterator can't be started more than once.");
        }

        iteratorStarted = true;

        return new Iterator<>() {
            int idx = 0;

            @Override
            public boolean hasNext() {
                return hasMorePages || idx < page.size();
            }

            @Override
            public SqlRow next() {
                if (idx == page.size()) {
                    if (!hasMorePages) {
                        throw new NoSuchElementException();
                    }

                    fetchNextPageInternal().join();
                    idx = 0;
                }

                return page.get(idx++);
            }
        };
    }
}
