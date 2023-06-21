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

package org.apache.ignite.internal.metastorage.impl;

import java.util.List;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.LocalMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * Local MetaStorage manager.
 */
public class LocalMetaStorageManagerImpl implements LocalMetaStorageManager {
    /** Actual storage for Meta storage. */
    private final KeyValueStorage storage;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

    /**
     * The constructor.
     *
     * @param storage Storage.
     * @param busyLock Busy lock to stop synchronously.
     */
    public LocalMetaStorageManagerImpl(KeyValueStorage storage, IgniteSpinBusyLock busyLock) {
        this.storage = storage;
        this.busyLock = busyLock;
    }

    @Override
    public List<Entry> get(byte[] key, long revLowerBound, long revUpperBound) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return storage.get(key, revLowerBound, revUpperBound);
        } finally {
            busyLock.leaveBusy();
        }
    }
}
