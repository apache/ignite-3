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

package org.apache.ignite.internal.pagememory.io;

import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.tostring.S;

/**
 * Data page payload.
 */
public class DataPagePayload {
    /** Offset. */
    private final int off;

    /** Payload size in bytes. */
    private final int payloadSize;

    /** Next link. */
    private final long nextLink;

    /**
     * Constructor.
     *
     * @param off Offset.
     * @param payloadSize Payload size in bytes.
     * @param nextLink Next link.
     */
    DataPagePayload(int off, int payloadSize, long nextLink) {
        this.off = off;
        this.payloadSize = payloadSize;
        this.nextLink = nextLink;
    }

    /**
     * Returns offset.
     */
    public int offset() {
        return off;
    }

    /**
     * Returns payload size.
     */
    public int payloadSize() {
        return payloadSize;
    }

    /**
     * Returns Link to the next fragment or {@code 0} if it is the last fragment or the data row is not fragmented.
     */
    public long nextLink() {
        return nextLink;
    }

    /**
     * Returns {@code true} if this payload links to next fragment.
     *
     * @return {@code true} if this payload links to next fragment
     */
    public boolean hasMoreFragments() {
        return nextLink != 0;
    }

    /**
     * Returns payload bytes.
     *
     * @param pageAddr Page address.
     */
    public byte[] getBytes(long pageAddr) {
        return PageUtils.getBytes(pageAddr, off, payloadSize);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(DataPagePayload.class, this);
    }
}

