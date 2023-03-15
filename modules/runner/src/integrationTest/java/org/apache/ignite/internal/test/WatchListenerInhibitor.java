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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.Watch;
import org.apache.ignite.internal.metastorage.server.WatchProcessor;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;

/**
 * Class for blocking Watch processing on a given Ignite node.
 */
public class WatchListenerInhibitor {
    /** "watches" field captured from the {@link RocksDbKeyValueStorage} instance. */
    private final Map<Watch, CompletableFuture<Void>> watches;

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

        var watches = (Map<Watch, CompletableFuture<Void>>) getFieldValue(watchProcessor, WatchProcessor.class, "watches");

        return new WatchListenerInhibitor(watches);
    }

    private WatchListenerInhibitor(Map<Watch, CompletableFuture<Void>> watches) {
        this.watches = watches;
    }

    /**
     * Starts inhibiting events.
     */
    public void startInhibit() {
        watches.replaceAll((watch, watchOperation) -> watchOperation.thenCompose(v -> inhibitFuture));
    }

    /**
     * Stops inhibiting events.
     */
    public void stopInhibit() {
        inhibitFuture.complete(null);
    }
}
