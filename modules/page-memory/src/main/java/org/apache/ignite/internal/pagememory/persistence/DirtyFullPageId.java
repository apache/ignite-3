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

import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;

/** No doc yet. */
// TODO: IGNITE-26233 Добавить документацию и все исправить
public class DirtyFullPageId extends FullPageId {
    private final int partitionGeneration;

    /** No doc yet. */
    public DirtyFullPageId(long pageId, int groupId, int partitionGeneration) {
        super(pageId, groupId);

        this.partitionGeneration = partitionGeneration;
    }

    public int partitionGeneration() {
        return partitionGeneration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof DirtyFullPageId)) {
            return false;
        }

        DirtyFullPageId that = (DirtyFullPageId) o;

        return effectivePageId() == that.effectivePageId() && groupId == that.groupId && partitionGeneration == that.partitionGeneration;
    }

    @Override
    public int hashCode() {
        return hashCode0(groupId, PageIdUtils.effectivePageId(pageId), partitionGeneration);
    }

    @Override
    public String toString() {
        return new IgniteStringBuilder("DirtyFullPageId [pageId=").appendHex(pageId)
                .app(", effectivePageId=").appendHex(effectivePageId())
                .app(", groupId=").app(groupId)
                .app(", partitionGeneration=").app(partitionGeneration)
                .app(']').toString();
    }

    private static int hashCode0(int groupId, long effectivePageId, int partitionGeneration) {
        return (int) (mix64(effectivePageId) ^ mix32(groupId) ^ mix32(partitionGeneration));
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 32)) * 0x4cd6944c5cc20b6dL;
        z = (z ^ (z >>> 29)) * 0xfc12c5b19d3259e9L;

        return z ^ (z >>> 32);
    }

    private static int mix32(int k) {
        k = (k ^ (k >>> 16)) * 0x85ebca6b;
        k = (k ^ (k >>> 13)) * 0xc2b2ae35;

        return k ^ (k >>> 16);
    }
}
