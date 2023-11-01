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

package org.apache.ignite.internal.pagememory.persistence.replacement;

import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.WriteDirtyPage;
import org.jetbrains.annotations.Nullable;

/**
 * Not thread safe and stateful class for page replacement of one page with write() delay. This allows to write page content without holding
 * segment lock. Page data is copied into temp buffer during {@link #write(PersistentPageMemory, FullPageId, ByteBuffer)} and then sent to
 * real implementation by {@link #finishReplacement}.
 */
public class DelayedDirtyPageWrite implements WriteDirtyPage {
    /** Real flush dirty page implementation. */
    private final WriteDirtyPage flushDirtyPage;

    /** Page size. */
    private final int pageSize;

    /** Thread local with byte buffers. */
    private final ThreadLocal<ByteBuffer> byteBufThreadLoc;

    /** Replacing pages tracker, used to register & unregister pages being written. */
    private final DelayedPageReplacementTracker tracker;

    /** Full page id to be written on {@link #finishReplacement} or {@code null} if nothing to write. */
    private @Nullable FullPageId fullPageId;

    /** Page memory to be used in {@link #finishReplacement}. */
    private @Nullable PersistentPageMemory pageMemory;

    /**
     * Constructor.
     *
     * @param flushDirtyPage real writer to save page to store.
     * @param byteBufThreadLoc thread local buffers to use for pages copying.
     * @param pageSize page size.
     * @param tracker tracker to lock/unlock page reads.
     */
    public DelayedDirtyPageWrite(
            WriteDirtyPage flushDirtyPage,
            ThreadLocal<ByteBuffer> byteBufThreadLoc,
            // TODO: IGNITE-17017 Move to common config
            int pageSize,
            DelayedPageReplacementTracker tracker
    ) {
        this.flushDirtyPage = flushDirtyPage;
        this.pageSize = pageSize;
        this.byteBufThreadLoc = byteBufThreadLoc;
        this.tracker = tracker;
    }

    /** {@inheritDoc} */
    @Override
    public void write(PersistentPageMemory pageMemory, FullPageId fullPageId, ByteBuffer byteBuf) {
        tracker.lock(fullPageId);

        ByteBuffer threadLocalBuf = byteBufThreadLoc.get();

        threadLocalBuf.rewind();

        long writeAddr = bufferAddress(threadLocalBuf);
        long origBufAddr = bufferAddress(byteBuf);

        copyMemory(origBufAddr, writeAddr, pageSize);

        this.fullPageId = fullPageId;
        this.pageMemory = pageMemory;
    }

    /**
     * Runs actual write if required. Method is 'no op' if there was no page selected for replacement.
     *
     * @throws IgniteInternalCheckedException if write failed.
     */
    public void finishReplacement() throws IgniteInternalCheckedException {
        if (fullPageId == null && pageMemory == null) {
            return;
        }

        try {
            flushDirtyPage.write(pageMemory, fullPageId, byteBufThreadLoc.get());
        } finally {
            tracker.unlock(fullPageId);

            fullPageId = null;
            pageMemory = null;
        }
    }
}
