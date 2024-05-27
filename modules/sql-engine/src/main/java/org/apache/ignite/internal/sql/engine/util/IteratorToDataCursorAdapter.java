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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.internal.util.AsyncWrapper;

/**
 * Adapter that converts {@link Iterator} to an instance of {@link AsyncDataCursor}.
 *
 * @param <T> Type of the items.
 */
public class IteratorToDataCursorAdapter<T> extends AsyncWrapper<T> implements AsyncDataCursor<T> {
    private final CompletableFuture<Void> initialized = new CompletableFuture<>();

    /**
     * Constructor.
     *
     * @param initFut Initialization future.
     * @param exec An executor to delegate execution.
     */
    public IteratorToDataCursorAdapter(CompletableFuture<Iterator<T>> initFut, Executor exec) {
        super(initFut, exec);

        initFut.whenComplete((r, e) -> {
            if (e != null) {
                initialized.completeExceptionally(e);
            } else {
                initialized.complete(null);
            }
        });
    }

    /**
     * Constructor.
     *
     * <p>The execution will be in the thread invoking particular method of this cursor.
     *
     * @param source An iterator to wrap.
     */
    public IteratorToDataCursorAdapter(Iterator<T> source) {
        this(CompletableFuture.completedFuture(source), Runnable::run);
    }

    @Override
    public CompletableFuture<Void> onClose() {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> onFirstPageReady() {
        return initialized;
    }
}
