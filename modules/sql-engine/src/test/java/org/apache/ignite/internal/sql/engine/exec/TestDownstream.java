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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.exec.rel.Downstream;

/**
 * Test implementations of {@link Downstream} that collects all rows in a list
 * and then completes a future with that list or with an exception in case {@link #onError(Throwable)}
 * was invoked on this downstream.
 *
 * @param <T> A type of the row.
 */
public class TestDownstream<T> implements Downstream<T> {
    private final List<T> rows = new ArrayList<>();
    private final CompletableFuture<List<T>> completion = new CompletableFuture<>();

    @Override
    public void push(T row) {
        rows.add(row);
    }

    @Override
    public void end() throws Exception {
        completion.complete(rows);
    }

    @Override
    public void onError(Throwable e) {
        completion.completeExceptionally(e);
    }

    public CompletableFuture<List<T>> result() {
        return completion;
    }
}
