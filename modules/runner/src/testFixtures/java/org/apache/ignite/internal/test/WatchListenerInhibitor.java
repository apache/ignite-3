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

package org.apache.ignite.internal.test;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getField;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.WatchProcessor;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;

/**
 * Class for blocking Watch processing on a given Ignite node.
 */
public class WatchListenerInhibitor {
    private final WatchProcessor watchProcessor;

    private final RocksDbKeyValueStorage storage;

    private final Field processorNotificationFutureField;

    private final Field storageRwLockField;

    /** Future used to block the watch notification thread. */
    private final CompletableFuture<Void> inhibitFuture = new CompletableFuture<>();

    /**
     * Creates the specific listener which can inhibit events for real metastorage listener.
     *
     * @param ignite Ignite.
     * @return Listener inhibitor.
     */
    public static WatchListenerInhibitor metastorageEventsInhibitor(Ignite ignite) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);

        return metastorageEventsInhibitor(igniteImpl.metaStorageManager());
    }

    /**
     * Creates the specific listener which can inhibit events for real metastorage listener.
     *
     * @param metaStorageManager Meta storage manager.
     * @return Listener inhibitor.
     */
    public static WatchListenerInhibitor metastorageEventsInhibitor(MetaStorageManager metaStorageManager) {
        var metaStorageManager0 = metaStorageManager;

        // TODO: IGNITE-15723 After a component factory is implemented, need to got rid of reflection here.
        var storage = (RocksDbKeyValueStorage) getFieldValue(metaStorageManager0, MetaStorageManagerImpl.class, "storage");

        var watchProcessor = (WatchProcessor) getFieldValue(storage, RocksDbKeyValueStorage.class, "watchProcessor");

        return new WatchListenerInhibitor(watchProcessor, storage);
    }

    private WatchListenerInhibitor(WatchProcessor watchProcessor, RocksDbKeyValueStorage storage) {
        this.watchProcessor = watchProcessor;
        this.storage = storage;

        processorNotificationFutureField = getField(watchProcessor, WatchProcessor.class, "notificationFuture");
        storageRwLockField = getField(storage, RocksDbKeyValueStorage.class, "rwLock");
    }

    /**
     * Starts inhibiting events.
     */
    public void startInhibit() {
        try {
            // We take this lock because it's actually used by RocksDbKeyValueStorage, among other things, to make future chaining
            // correct wrt concurrency.
            ReadWriteLock rwLock = (ReadWriteLock) storageRwLockField.get(storage);

            rwLock.writeLock().lock();

            try {
                CompletableFuture<Void> notificationFuture = (CompletableFuture<Void>) processorNotificationFutureField.get(watchProcessor);

                processorNotificationFutureField.set(watchProcessor, notificationFuture.thenCompose(v -> inhibitFuture));
            } finally {
                rwLock.writeLock().unlock();
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Stops inhibiting events.
     */
    public void stopInhibit() {
        inhibitFuture.complete(null);
    }

    /**
     * Executes an action enclosed in watch inhibition: that is, before execution inhibition gets started, and after the execution
     * it gets stopped.
     *
     * @param action Action to execute.
     * @return Action result.
     */
    public <T> T withInhibition(Supplier<? extends T> action) {
        startInhibit();

        try {
            return action.get();
        } finally {
            stopInhibit();
        }
    }

    /**
     * Executes an action enclosed in watch inhibition: that is, before execution inhibition gets started, and after the execution
     * it gets stopped.
     *
     * @param action Action to execute.
     */
    public void withInhibition(Runnable action) {
        startInhibit();

        try {
            action.run();
        } finally {
            stopInhibit();
        }
    }

    /**
     * Executes an action enclosed in watch inhibition: that is, before execution inhibition gets started, and after the execution
     * it gets stopped.
     *
     * @param ignite Node on which to inhibit watch processing.
     * @param action Action to execute.
     * @return Action result.
     */
    public static <T> T withInhibition(Ignite ignite, Supplier<? extends T> action) {
        return metastorageEventsInhibitor(ignite).withInhibition(action);
    }
}
