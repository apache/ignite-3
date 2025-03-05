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

import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.jetbrains.annotations.Nullable;

/**
 * Adds a {@link PageLockListener} to an {@link OffheapReadWriteLock}.
 */
public class ListeningOffheapReadWriteLock extends OffheapReadWriteLock {
    private final OffheapReadWriteLock delegate;
    private final PageLockListener listener;

    /**
     * Constructor.
     *
     * @param delegate Delegate.
     * @param listener Listener.
     */
    public ListeningOffheapReadWriteLock(OffheapReadWriteLock delegate, PageLockListener listener) {
        super(1);
        this.delegate = delegate;
        this.listener = listener;
    }

    @Override
    public boolean readLock(long lock, int tag) {
        listener.onBeforeReadLock(lock);

        boolean locked = delegate.readLock(lock, tag);

        listener.onReadLock(lock, locked);

        return locked;
    }

    @Override
    public void readUnlock(long lock) {
        listener.onReadUnlock(lock);

        delegate.readUnlock(lock);
    }

    @Override
    public boolean tryWriteLock(long lock, int tag) {
        listener.onBeforeWriteLock(lock);

        boolean locked = delegate.tryWriteLock(lock, tag);

        listener.onWriteLock(lock, locked);

        return locked;
    }

    @Override
    public boolean writeLock(long lock, int tag) {
        listener.onBeforeWriteLock(lock);

        boolean locked = delegate.writeLock(lock, tag);

        listener.onWriteLock(lock, locked);

        return locked;
    }

    @Override
    public boolean isWriteLocked(long lock) {
        return delegate.isWriteLocked(lock);
    }

    @Override
    public boolean isReadLocked(long lock) {
        return delegate.isReadLocked(lock);
    }

    @Override
    public void writeUnlock(long lock, int tag) {
        listener.onWriteUnlock(lock);

        delegate.writeUnlock(lock, tag);
    }

    @Override
    public @Nullable Boolean upgradeToWriteLock(long lock, int tag) {
        return delegate.upgradeToWriteLock(lock, tag);
    }
}
