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

package org.apache.ignite.internal.pagememory.util;

import org.apache.ignite.internal.close.ManuallyCloseable;

/**
 * Page lock listener.
 */
// TODO IGNITE-16350 Consider froper Before/After naming convention for all methods in this class.
public interface PageLockListener extends ManuallyCloseable {
    /**
     * Callback that's called before write lock acquiring.
     *
     * @param lockAddress Lock pointer.
     */
    void onBeforeWriteLock(long lockAddress);

    /**
     * Callback that's called after lock acquiring.
     *
     * @param lockAddress Lock pointer.
     * @param locked {@code true} if lock is locked.
     */
    void onWriteLock(long lockAddress, boolean locked);

    /**
     * Callback that's called before write lock releasing.
     *
     * @param lockAddress Lock pointer.
     */
    void onWriteUnlock(long lockAddress);

    /**
     * Callback that's called before read lock acquiring.
     *
     * @param lockAddress Lock pointer.
     */
    void onBeforeReadLock(long lockAddress);

    /**
     * Callback that's called after read lock acquiring.
     *
     * @param lockAddress Lock pointer.
     * @param locked {@code true} if lock is locked.
     */
    void onReadLock(long lockAddress, boolean locked);

    /**
     * Callback that's called before read lock releasing.
     *
     * @param lockAddress Lock pointer.
     */
    void onReadUnlock(long lockAddress);

    /** {@inheritDoc} */
    @Override
    void close();
}
