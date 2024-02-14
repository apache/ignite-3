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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.cache.CacheStore;
import org.apache.ignite.cache.CacheStoreSession;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * No-op cache store.
 *
 * @param <K>
 * @param <V>
 */
public class EmptyCacheStore implements CacheStore {
    @Override
    public CacheStoreSession beginSession() {
        return null;
    }

    @Override
    public CompletableFuture<Tuple> load(Tuple key) throws IgniteException {
        return CompletableFutures.nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Map<Tuple, Tuple>> loadAll(Iterable<? extends Tuple> keys) throws IgniteException {
        return null;
    }

    @Override
    public void write(@Nullable CacheStoreSession session, Entry<? extends Tuple, ? extends Tuple> entry) {
    }

    @Override
    public void writeAll(@Nullable CacheStoreSession session, Collection<Entry<? extends Tuple, ? extends Tuple>> entries) {

    }

    @Override
    public void delete(@Nullable CacheStoreSession session, Object key) {
    }

    @Override
    public void deleteAll(@Nullable CacheStoreSession session, Collection<?> keys) {

    }
}
