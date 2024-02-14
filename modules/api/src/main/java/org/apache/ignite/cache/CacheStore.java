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

package org.apache.ignite.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

public interface CacheStore {
    CacheStoreSession beginSession();

    CompletableFuture<Tuple> load(Tuple key);

    CompletableFuture<Map<Tuple, Tuple>> loadAll(Iterable<? extends Tuple> keys);

    void write(@Nullable CacheStoreSession session, Entry<? extends Tuple, ? extends Tuple> entry);

    void writeAll(@Nullable CacheStoreSession session, Collection<Entry<? extends Tuple, ? extends Tuple>> entries);

    void delete(@Nullable CacheStoreSession session, Object key);

    void deleteAll(@Nullable CacheStoreSession session, Collection<?> keys);
}
