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

package org.apache.ignite.internal.storage.pagememory.mv.io;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;

import org.apache.ignite.internal.pagememory.io.PageIo;

/**
 * Base for {@link PageIo} implementations working with blobs.
 */
public abstract class BlobIo extends PageIo {
    protected BlobIo(int type, int ver) {
        super(type, ver, FLAG_AUX);
    }

    /**
     * Returns full header size in bytes.
     */
    public abstract int fullHeaderSize();

    /**
     * Reads next page ID.
     */
    public abstract long getNextPageId(long pageAddr);

    /**
     * Writes next page ID.
     */
    public abstract void setNextPageId(long pageAddr, long nextPageId);

    /**
     * Reads total blob length.
     */
    public int getTotalLength(long pageAddr) {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes total blob length.
     */
    public void setTotalLength(long pageAddr, int totalLength) {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads current fragment length.
     */
    public abstract int getFragmentLength(long pageAddr);

    /**
     * Sets current fragment length.
     */
    public abstract void setFragmentLength(long pageAddr, int fragmentLength);

    /**
     * Reads fragment bytes to the given array.
     */
    public abstract void getFragmentBytes(long pageAddr, byte[] destArray, int destOffset, int fragmentLength);

    /**
     * Writes fragment bytes from the given array.
     */
    public abstract void setFragmentBytes(long pageAddr, byte[] bytes, int bytesOffset, int fragmentLength);
}
