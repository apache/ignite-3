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

package org.apache.ignite.internal.pagememory;

import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteStringBuilder;

/**
 * Compound object used to address a page in the global page space.
 * <h3>Page ID structure</h3>
 * <p>
 * Generally, a full page ID consists of a group ID and page ID. A page ID consists of page index (32 bits), partition ID (16 bits) and
 * flags. Higher 8 bits of page ID are unused and reserved to address entries inside data pages or page ID rotation.
 * <p>
 * Partition ID {@code 0xFFFF} is reserved for index pages.
 * <p>
 * The structure of a page ID is shown in the diagram below:
 * <pre>
 * +---------+-----------+------------+--------------------------+
 * | 8 bits  |   8 bits  |  16 bits   |         32 bits          |
 * +---------+-----------+------------+--------------------------+
 * |  OFFSET |   FLAGS   |PARTITION ID|       PAGE INDEX         |
 * +---------+-----------+------------+--------------------------+
 * <p>
 * <h3>Page ID rotation</h3>
 * There are scenarios when we reference one page (B) from within another page (A) by page ID. It is also
 * possible that this first page (B) is concurrently reused for a different purpose. In this
 * case we should have a mechanism to determine that the reference from page (A) to page (B) is no longer valid.
 * This is ensured by page ID rotation - together with page's (B) ID we should write some value that is incremented
 * each time a page is reused (page ID rotation). This ID should be verified after page read and a page
 * should be discarded if full ID is different.
 * <p>
 * Effective page ID is page ID with zeroed bits used for page ID rotation.
 */
public class FullPageId {
    /** */
    public static final FullPageId NULL_PAGE = new FullPageId(-1, -1);

    /** Page ID. */
    private final long pageId;

    /** */
    private final long effectivePageId;

    /** Group ID. */
    private final int groupId;

    /**
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @return Hash code.
     */
    public static int hashCode(int groupId, long pageId) {
        long effectiveId = PageIdUtils.effectivePageId(pageId);

        return IgniteUtils.hash(hashCode0(groupId, effectiveId));
    }

    /**
     * Will not clear link bits.
     *
     * @param groupId         Group ID.
     * @param effectivePageId Effective page ID.
     * @return Hash code.
     */
    private static int hashCode0(int groupId, long effectivePageId) {
        return (int) (mix64(effectivePageId) ^ mix32(groupId));
    }

    /**
     * MH3's plain finalization step.
     */
    private static int mix32(int k) {
        k = (k ^ (k >>> 16)) * 0x85ebca6b;
        k = (k ^ (k >>> 13)) * 0xc2b2ae35;

        return k ^ (k >>> 16);
    }

    /**
     * Computes David Stafford variant 9 of 64bit mix function (MH3 finalization step, with different shifts and constants).
     * <p>
     * Variant 9 is picked because it contains two 32-bit shifts which could be possibly optimized into better machine code.
     *
     * @see "http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html"
     */
    private static long mix64(long z) {
        z = (z ^ (z >>> 32)) * 0x4cd6944c5cc20b6dL;
        z = (z ^ (z >>> 29)) * 0xfc12c5b19d3259e9L;

        return z ^ (z >>> 32);
    }

    /**
     * @param pageId  Page ID.
     * @param groupId Group ID.
     */
    public FullPageId(long pageId, int groupId) {
        this.pageId = pageId;
        this.groupId = groupId;

        effectivePageId = PageIdUtils.effectivePageId(pageId);
    }

    /**
     * @return Page ID.
     */
    public long pageId() {
        return pageId;
    }

    /**
     * @return Effective page ID.
     */
    public long effectivePageId() {
        return effectivePageId;
    }

    /**
     * @return Group ID.
     */
    public int groupId() {
        return groupId;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof FullPageId)) {
            return false;
        }

        FullPageId that = (FullPageId) o;

        return effectivePageId == that.effectivePageId && groupId == that.groupId;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return hashCode0(groupId, effectivePageId);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return new IgniteStringBuilder("FullPageId [pageId=").appendHex(pageId)
                .app(", effectivePageId=").appendHex(effectivePageId)
                .app(", groupId=").app(groupId).app(']').toString();
    }
}
