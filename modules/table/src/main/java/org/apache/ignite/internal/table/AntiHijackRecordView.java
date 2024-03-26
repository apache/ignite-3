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

package org.apache.ignite.internal.table;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link RecordView} that adds anti-thread-hijacking protection to methods returning {@link CompletableFuture}s.
 *
 * @see PublicApiThreading#preventThreadHijack(CompletableFuture, Executor)
 */
public class AntiHijackRecordView<R> extends AntiHijackViewBase<R> implements RecordView<R>, Wrapper {
    private final RecordView<R> view;

    /**
     * Constructor.
     *
     * @param view View to wrap.
     * @param asyncContinuationExecutor Executor to which execution will be resubmitted when leaving asynchronous public API endpoints
     *     (to prevent the user from stealing Ignite threads).
     */
    public AntiHijackRecordView(RecordView<R> view, Executor asyncContinuationExecutor) {
        super(view, view, asyncContinuationExecutor);

        this.view = view;
    }

    @Override
    public R get(@Nullable Transaction tx, R keyRec) {
        return view.get(tx, keyRec);
    }

    @Override
    public CompletableFuture<R> getAsync(@Nullable Transaction tx, R keyRec) {
        return preventThreadHijack(view.getAsync(tx, keyRec));
    }

    @Override
    public List<R> getAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return view.getAll(tx, keyRecs);
    }

    @Override
    public CompletableFuture<List<R>> getAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        return preventThreadHijack(view.getAllAsync(tx, keyRecs));
    }

    @Override
    public boolean contains(@Nullable Transaction tx, R keyRec) {
        return view.contains(tx, keyRec);
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, R keyRec) {
        return preventThreadHijack(view.containsAsync(tx, keyRec));
    }

    @Override
    public void upsert(@Nullable Transaction tx, R rec) {
        view.upsert(tx, rec);
    }

    @Override
    public CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, R rec) {
        return preventThreadHijack(view.upsertAsync(tx, rec));
    }

    @Override
    public void upsertAll(@Nullable Transaction tx, Collection<R> recs) {
        view.upsertAll(tx, recs);
    }

    @Override
    public CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        return preventThreadHijack(view.upsertAllAsync(tx, recs));
    }

    @Override
    public R getAndUpsert(@Nullable Transaction tx, R rec) {
        return view.getAndUpsert(tx, rec);
    }

    @Override
    public CompletableFuture<R> getAndUpsertAsync(@Nullable Transaction tx, R rec) {
        return preventThreadHijack(view.getAndUpsertAsync(tx, rec));
    }

    @Override
    public boolean insert(@Nullable Transaction tx, R rec) {
        return view.insert(tx, rec);
    }

    @Override
    public CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, R rec) {
        return preventThreadHijack(view.insertAsync(tx, rec));
    }

    @Override
    public List<R> insertAll(@Nullable Transaction tx, Collection<R> recs) {
        return view.insertAll(tx, recs);
    }

    @Override
    public CompletableFuture<List<R>> insertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        return preventThreadHijack(view.insertAllAsync(tx, recs));
    }

    @Override
    public boolean replace(@Nullable Transaction tx, R rec) {
        return view.replace(tx, rec);
    }

    @Override
    public boolean replace(@Nullable Transaction tx, R oldRec, R newRec) {
        return view.replace(tx, oldRec, newRec);
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, R rec) {
        return preventThreadHijack(view.replaceAsync(tx, rec));
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, R oldRec, R newRec) {
        return preventThreadHijack(view.replaceAsync(tx, oldRec, newRec));
    }

    @Override
    public R getAndReplace(@Nullable Transaction tx, R rec) {
        return view.getAndReplace(tx, rec);
    }

    @Override
    public CompletableFuture<R> getAndReplaceAsync(@Nullable Transaction tx, R rec) {
        return preventThreadHijack(view.getAndReplaceAsync(tx, rec));
    }

    @Override
    public boolean delete(@Nullable Transaction tx, R keyRec) {
        return view.delete(tx, keyRec);
    }

    @Override
    public CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, R keyRec) {
        return preventThreadHijack(view.deleteAsync(tx, keyRec));
    }

    @Override
    public boolean deleteExact(@Nullable Transaction tx, R rec) {
        return view.deleteExact(tx, rec);
    }

    @Override
    public CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, R rec) {
        return preventThreadHijack(view.deleteExactAsync(tx, rec));
    }

    @Override
    public R getAndDelete(@Nullable Transaction tx, R keyRec) {
        return view.getAndDelete(tx, keyRec);
    }

    @Override
    public CompletableFuture<R> getAndDeleteAsync(@Nullable Transaction tx, R keyRec) {
        return preventThreadHijack(view.getAndDeleteAsync(tx, keyRec));
    }

    @Override
    public List<R> deleteAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return view.deleteAll(tx, keyRecs);
    }

    @Override
    public CompletableFuture<List<R>> deleteAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        return preventThreadHijack(view.deleteAllAsync(tx, keyRecs));
    }

    @Override
    public List<R> deleteAllExact(@Nullable Transaction tx, Collection<R> recs) {
        return view.deleteAllExact(tx, recs);
    }

    @Override
    public CompletableFuture<List<R>> deleteAllExactAsync(@Nullable Transaction tx, Collection<R> recs) {
        return preventThreadHijack(view.deleteAllExactAsync(tx, recs));
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return Wrappers.unwrap(view, classToUnwrap);
    }
}
