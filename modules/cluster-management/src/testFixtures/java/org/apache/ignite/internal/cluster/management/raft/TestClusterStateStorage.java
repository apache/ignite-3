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

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * {@link ClusterStateStorage} in-memory implementation based on a {@link ConcurrentHashMap}.
 */
public class TestClusterStateStorage implements ClusterStateStorage {
    private static final String SNAPSHOT_FILE = "snapshot.bin";

    private final Map<ByteArray, byte[]> map = new HashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile boolean isStarted = false;

    @Override
    public CompletableFuture<Void> start() {
        isStarted = true;

        return nullCompletedFuture();
    }

    @Override
    public byte @Nullable [] get(byte[] key) {
        lock.readLock().lock();

        try {
            return map.get(new ByteArray(key));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        lock.writeLock().lock();

        try {
            map.put(new ByteArray(key), value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void replaceAll(byte[] prefix, byte[] key, byte[] value) {
        lock.writeLock().lock();

        try {
            map.entrySet().removeIf(e -> startsWith(e.getKey().bytes(), prefix));

            map.put(new ByteArray(key), value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void remove(byte[] key) {
        lock.writeLock().lock();

        try {
            map.remove(new ByteArray(key));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void removeAll(Collection<byte[]> keys) {
        lock.writeLock().lock();

        try {
            for (byte[] key : keys) {
                remove(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public <T> Cursor<T> getWithPrefix(byte[] prefix, BiFunction<byte[], byte[], T> entryTransformer) {
        lock.readLock().lock();

        try {
            return map.entrySet().stream()
                    .filter(e -> startsWith(e.getKey().bytes(), prefix))
                    .map(e -> entryTransformer.apply(e.getKey().bytes(), e.getValue()))
                    .collect(collectingAndThen(toList(), data -> Cursor.fromBareIterator(data.iterator())));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        List<byte[]> keys;
        List<byte[]> values;

        lock.readLock().lock();

        try {
            keys = new ArrayList<>(map.size());
            values = new ArrayList<>(map.size());

            map.forEach((k, v) -> {
                keys.add(k.bytes());
                values.add(v);
            });
        } finally {
            lock.readLock().unlock();
        }

        return CompletableFuture.runAsync(() -> {
            try (var out = new ObjectOutputStream(Files.newOutputStream(snapshotPath.resolve(SNAPSHOT_FILE)))) {
                out.writeObject(keys);
                out.writeObject(values);
            } catch (Exception e) {
                throw new IgniteInternalException(e);
            }
        });
    }

    @Override
    public void restoreSnapshot(Path snapshotPath) {
        try (var in = new ObjectInputStream(Files.newInputStream(snapshotPath.resolve(SNAPSHOT_FILE)))) {
            var keys = (List<byte[]>) in.readObject();
            var values = (List<byte[]>) in.readObject();

            lock.writeLock().lock();

            try {
                map.clear();

                for (int i = 0; i < keys.size(); i++) {
                    map.put(new ByteArray(keys.get(i)), values.get(i));
                }
            } finally {
                lock.writeLock().unlock();
            }
        } catch (Exception e) {
            throw new IgniteInternalException(e);
        }
    }

    @Override
    public void destroy() {
        lock.writeLock().lock();

        try {
            map.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void stop() {
        isStarted = false;
    }
}
