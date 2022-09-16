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

/**
 * {@link PageLockListener} implementation that does nothing.
 */
public class PageLockListenerNoOp implements PageLockListener {
    /** Instance. */
    public static final PageLockListenerNoOp INSTANCE = new PageLockListenerNoOp();

    /**
     * Private constructor.
     */
    private PageLockListenerNoOp() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void onBeforeWriteLock(int groupId, long pageId, long page) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void onWriteLock(int groupId, long pageId, long page, long pageAddr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void onWriteUnlock(int groupId, long pageId, long page, long pageAddr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void onBeforeReadLock(int groupId, long pageId, long page) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void onReadLock(int groupId, long pageId, long page, long pageAddr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void onReadUnlock(int groupId, long pageId, long page, long pageAddr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        // No-op.
    }
}
