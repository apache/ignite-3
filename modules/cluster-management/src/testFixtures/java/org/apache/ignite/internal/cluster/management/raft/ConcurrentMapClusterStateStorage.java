/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.cluster.management.raft;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * {@link ClusterStateStorage} in-memory implementation based on a {@link ConcurrentHashMap}.
 */
public class ConcurrentMapClusterStateStorage implements ClusterStateStorage {
    private static final String SNAPSHOT_FILE = "snapshot.bin";

    private final Map<ByteArray, byte[]> map = new ConcurrentHashMap<>();

    private volatile boolean isStarted = false;

    @Override
    public void start() {
        isStarted = true;
    }

    @Override
    public boolean isStarted() {
        return isStarted;
    }

    @Override
    public byte @Nullable [] get(byte[] key) {
        return map.get(new ByteArray(key));
    }

    @Override
    public void put(byte[] key, byte[] value) {
        map.put(new ByteArray(key), value);
    }

    @Override
    public void remove(byte[] key) {
        map.remove(new ByteArray(key));
    }

    @Override
    public void removeAll(Collection<byte[]> keys) {
        for (byte[] key : keys) {
            remove(key);
        }
    }

    @Override
    public <T> Cursor<T> getWithPrefix(byte[] prefix, BiFunction<byte[], byte[], T> entryTransformer) {
        Iterator<T> it = map.entrySet().stream()
                .filter(e -> {
                    byte[] key = e.getKey().bytes();

                    if (key.length < prefix.length) {
                        return false;
                    }

                    return Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
                })
                .map(e -> entryTransformer.apply(e.getKey().bytes(), e.getValue()))
                .iterator();

        return new Cursor<>() {
            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                return it.next();
            }
        };
    }

    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        var keys = new ArrayList<byte[]>(map.size());
        var values = new ArrayList<byte[]>(map.size());

        map.forEach((k, v) -> {
            keys.add(k.bytes());
            values.add(v);
        });

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

            map.clear();

            for (int i = 0; i < keys.size(); i++) {
                map.put(new ByteArray(keys.get(i)), values.get(i));
            }
        } catch (Exception e) {
            throw new IgniteInternalException(e);
        }
    }

    @Override
    public void destroy() {
        map.clear();
    }

    @Override
    public void close() throws Exception {
        isStarted = false;
    }
}
