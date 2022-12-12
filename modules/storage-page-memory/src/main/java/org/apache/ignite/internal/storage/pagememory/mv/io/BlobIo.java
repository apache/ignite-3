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

public abstract class BlobIo extends PageIo {
    protected BlobIo(int type, int ver) {
        super(type, ver, FLAG_AUX);
    }

    public abstract int fullHeaderSize();

    public abstract long getNextPageId(long pageAddr);

    public abstract void setNextPageId(long pageAddr, long nextPageId);

    public int getTotalLength(long pageAddr) {
        throw new UnsupportedOperationException();
    }

    public void setTotalLength(long pageAddr, int totalLength) {
        throw new UnsupportedOperationException();
    }

    public abstract int getFragmentLength(long pageAddr);

    public abstract void setFragmentLength(long pageAddr, int fragmentLength);

    public abstract void setFragmentBytes(long pageAddr, byte[] bytes, int bytesOffset, int fragmentLength);
}
