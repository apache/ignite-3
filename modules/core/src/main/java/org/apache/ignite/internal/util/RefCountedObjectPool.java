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

package org.apache.ignite.internal.util;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A pool of objects that are reference counted. Objects are created on demand and destroyed when the reference count
 * reaches zero.
 *
 * @param <K> The type of the key used to identify the object.
 * @param <V> The type of the object.
 */
public class RefCountedObjectPool<K, V> {
    /**
     * The number of references to each object.
     */
    private final Map<K, Integer> refCount = new HashMap<>();

    /**
     * The objects that are reference counted.
     */
    private final Map<K, V> refCountedObjects = new HashMap<>();

    /**
     * Acquires an object from the pool. If the object does not exist, it is created using the provided supplier.
     *
     * @param key            The key used to identify the object.
     * @param objectSupplier The supplier used to create the object if it does not exist.
     * @return The object.
     */
    public synchronized V acquire(K key, Function<K, ? extends V> objectSupplier) {
        int currentRefCount = refCount.getOrDefault(key, 0);
        refCount.put(key, currentRefCount + 1);
        return refCountedObjects.computeIfAbsent(key, objectSupplier);
    }

    /**
     * Returns true if the object is acquired.
     *
     * @param key The key used to identify the object.
     * @return True if the object is acquired.
     */
    public synchronized boolean isAcquired(K key) {
        return refCount.containsKey(key);
    }

    /**
     * Releases an object from the pool. If the reference count reaches zero, the object is destroyed.
     * Returns true if the object was destroyed.
     *
     * @param key The key used to identify the object.
     * @return True if the object was destroyed.
     */
    public synchronized boolean release(K key) {
        int currentRefCount = refCount.getOrDefault(key, 0);
        int newRefCount = currentRefCount - 1;

        if (newRefCount < 0) {
            throw new IllegalStateException("Object " + key + " is not acquired");
        } else if (newRefCount > 0) {
            refCount.put(key, newRefCount);
            return false;
        } else {
            refCount.remove(key);
            refCountedObjects.remove(key);
            return true;
        }
    }
}
