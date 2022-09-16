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

package org.apache.ignite.internal.pagememory.persistence;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;

/**
 * Implementation for tests.
 */
public class TestPageReadWriteManager implements PageReadWriteManager {
    private final ConcurrentMap<FullPageId, AtomicInteger> allocators = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override
    public void read(int grpId, long pageId, ByteBuffer pageBuf, boolean keepCrc) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public PageStore write(int grpId, long pageId, ByteBuffer pageBuf, boolean calculateCrc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public long allocatePage(int grpId, int partId, byte flags) {
        long root = pageId(partId, flags, 0);

        FullPageId fullId = new FullPageId(root, grpId);

        AtomicInteger allocator = allocators.computeIfAbsent(fullId, fullPageId -> new AtomicInteger(1));

        return pageId(partId, flags, allocator.getAndIncrement());
    }
}
