/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.client.handler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.lang.IgniteException;

/**
 * Per-connection resource registry.
 */
public class ClientResourceRegistry {
    /** Handles. */
    private final Map<Long, ClientResource> res = new ConcurrentHashMap<>();

    /** ID generator. */
    private final AtomicLong idGen = new AtomicLong();

    /**
     * Allocates server handle for an object.
     *
     * @param obj Object.
     * @return Handle.
     */
    public long put(ClientResource obj) {
        long id = idGen.incrementAndGet();

        res.put(id, obj);

        return id;
    }

    /**
     * Gets the object by handle.
     *
     * @param hnd Handle.
     * @return Object.
     */
    public ClientResource get(long hnd) {
        ClientResource res = this.res.get(hnd);

        if (res == null) {
            throw new IgniteException("Failed to find resource with id: " + hnd);
        }

        return res;
    }

    /**
     * Releases the handle.
     *
     * @param hnd Handle.
     */
    public ClientResource remove(long hnd) {
        ClientResource res = this.res.remove(hnd);

        if (res == null) {
            throw new IgniteException("Failed to find resource with id: " + hnd);
        }

        return res;
    }

    /**
     * Cleans all handles and closes all ClientCloseableResources.
     */
    public void clean() {
        IgniteException ex = null;

        for (ClientResource r : res.values()) {
            try {
                r.release();
            } catch (Exception e) {
                if (ex == null) {
                    ex = new IgniteException(e);
                } else {
                    ex.addSuppressed(e);
                }
            }
        }

        if (ex != null) {
            throw ex;
        }
    }
}
