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

package org.apache.ignite.client.handler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.IgniteException;

/**
 * Per-connection resource registry.
 */
public class ClientResourceRegistry {
    /**
     * Resources.
     */
    private final Map<Long, ClientResource> res = new ConcurrentHashMap<>();

    /**
     * ID generator.
     */
    private final AtomicLong idGen = new AtomicLong();

    /**
     * RW lock.
     */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Stores the resource and returns the generated id.
     *
     * @param obj Object.
     * @return Id.
     */
    public long put(ClientResource obj) throws IgniteInternalCheckedException {
        enter();

        try {
            long id = idGen.incrementAndGet();

            res.put(id, obj);

            return id;
        } finally {
            leave();
        }
    }

    /**
     * Gets the resource by id.
     *
     * @param id Id.
     * @return Object.
     */
    public ClientResource get(long id) throws IgniteInternalCheckedException {
        enter();

        try {
            ClientResource res = this.res.get(id);

            if (res == null) {
                throw new IgniteException(Client.RESOURCE_NOT_FOUND_ERR, "Failed to find resource with id: " + id);
            }

            return res;
        } finally {
            leave();
        }
    }

    /**
     * Removes the resource.
     *
     * @param id Id.
     */
    public ClientResource remove(long id) throws IgniteInternalCheckedException {
        enter();

        try {
            ClientResource res = this.res.remove(id);

            if (res == null) {
                throw new IgniteException(Client.RESOURCE_NOT_FOUND_ERR, "Failed to find resource with id: " + id);
            }

            return res;
        } finally {
            leave();
        }
    }

    /**
     * Closes the registry and releases all resources.
     */
    public void close() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        IgniteInternalException ex = null;

        for (ClientResource r : res.values()) {
            try {
                r.release();
            } catch (Exception e) {
                if (ex == null) {
                    ex = new IgniteInternalException(e);
                } else {
                    ex.addSuppressed(e);
                }
            }
        }

        res.clear();

        if (ex != null) {
            throw ex;
        }
    }

    /**
     * Enters the lock.
     */
    private void enter() throws IgniteInternalCheckedException {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalCheckedException("Resource registry is closed.");
        }
    }

    /**
     * Leaves the lock.
     */
    private void leave() {
        busyLock.leaveBusy();
    }
}
