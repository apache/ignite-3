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

package org.apache.ignite.internal.pagememory.reuse;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import java.io.Externalizable;

/**
 * Reuse bag based on a list of long.
 */
public final class LongListReuseBag implements ReuseBag {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    private final LongArrayFIFOQueue pages = new LongArrayFIFOQueue();

    /**
     * Default constructor for {@link Externalizable}.
     */
    public LongListReuseBag() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void addFreePage(long pageId) {
        pages.enqueue(pageId);
    }

    /** {@inheritDoc} */
    @Override
    public long pollFreePage() {
        return isEmpty() ? 0 : pages.dequeueLastLong();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
        return pages.isEmpty();
    }

    /**
     * Returns the number of pages in this reuse bag.
     */
    public int size() {
        return pages.size();
    }
}

