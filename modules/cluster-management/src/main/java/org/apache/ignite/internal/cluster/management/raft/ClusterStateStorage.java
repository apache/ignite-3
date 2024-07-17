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

package org.apache.ignite.internal.cluster.management.raft;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.jetbrains.annotations.Nullable;

/**
 * Storage for the CMG Raft service.
 */
public interface ClusterStateStorage extends IgniteComponent {
    /**
     * Retrieves a value associated with the given key or {@code null} if no such value exists.
     *
     * @param key Key, which associated value should be retrieved.
     * @return Value associated with the given key or {@code null} if no such value exists.
     */
    byte @Nullable [] get(byte[] key);

    /**
     * Adds a new key-value pair to the storage.
     *
     * @param key Key.
     * @param value Value.
     */
    void put(byte[] key, byte[] value);

    /**
     * Atomically removes all keys starting with the given prefix and inserts a new value associated with the given key.
     *
     * @param prefix Key prefix that should be removed.
     * @param key Key to insert.
     * @param value Value to insert.
     */
    void replaceAll(byte[] prefix, byte[] key, byte[] value);

    /**
     * Removes the value associated with the given key. Does nothing if no such association exists.
     *
     * @param key Key which value should be removed.
     */
    void remove(byte[] key);

    /**
     * Removes all values associated with the given keys. Keys that are not present in the storage are skipped.
     *
     * @param keys Keys which values should be removed.
     */
    void removeAll(Collection<byte[]> keys);

    /**
     * Creates a list containing a range of keys, starting with the given prefix.
     *
     * @param prefix Key prefix.
     * @param entryTransformer Entry transformation function.
     * @param <T> Type of converted entry.
     * @return List containing a range of existing keys.
     */
    <T> List<T> getWithPrefix(byte[] prefix, BiFunction<byte[], byte[], T> entryTransformer);

    /**
     * Creates a snapshot of the storage's current state in the specified directory.
     *
     * @param snapshotPath Directory to store a snapshot.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> snapshot(Path snapshotPath);

    /**
     * Restores a state of the storage which was previously captured with a {@link #snapshot(Path)}.
     *
     * @param snapshotPath Path to the snapshot's directory.
     */
    void restoreSnapshot(Path snapshotPath);
}
