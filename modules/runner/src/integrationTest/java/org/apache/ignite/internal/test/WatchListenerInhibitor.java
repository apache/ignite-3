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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.getField;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.WatchProcessor;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;

/**
 * Class for blocking Watch processing on a given Ignite node.
 */
public class WatchListenerInhibitor {
    private final WatchProcessor watchProcessor;

    private final Field notificationFutureField;

    /** Future used to block the watch notification thread. */
    private final CompletableFuture<Void> inhibitFuture = new CompletableFuture<>();

    /**
     * Creates the specific listener which can inhibit events for real metastorage listener.
     *
     * @param ignite Ignite.
     * @return Listener inhibitor.
     */
    public static WatchListenerInhibitor metastorageEventsInhibitor(Ignite ignite) {
        //TODO: IGNITE-15723 After a component factory will be implemented, need to got rid of reflection here.
        var metaStorageManager = (MetaStorageManagerImpl) getFieldValue(ignite, IgniteImpl.class, "metaStorageMgr");

        var storage = (RocksDbKeyValueStorage) getFieldValue(metaStorageManager, MetaStorageManagerImpl.class, "storage");

        var watchProcessor = (WatchProcessor) getFieldValue(storage, RocksDbKeyValueStorage.class, "watchProcessor");

        return new WatchListenerInhibitor(watchProcessor);
    }

    private WatchListenerInhibitor(WatchProcessor watchProcessor) {
        this.watchProcessor = watchProcessor;
        this.notificationFutureField = getField(watchProcessor, WatchProcessor.class, "notificationFuture");
    }

    /**
     * Starts inhibiting events.
     */
    public void startInhibit() {
        try {
            CompletableFuture<Void> notificationFuture = (CompletableFuture<Void>) notificationFutureField.get(watchProcessor);

            notificationFutureField.set(watchProcessor, notificationFuture.thenCompose(v -> inhibitFuture));
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
}
