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

package org.apache.ignite.internal.storage;

import org.jetbrains.annotations.NotNull;

/**
 * Represents a node in the write intent list, which tracks uncommitted changes in a partition.
 *
 * <p>Each node contains a {@link RowId} identifying the row, as well as links to the previous and next nodes
 * in the list. The {@code link} field provides physical link of the row version in the partition.
 */
public class WriteIntentListNode {
    public static WriteIntentListNode EMPTY = new WriteIntentListNode(null, 0, 0, 0);

    public final RowId rowId;
    public final long link;
    public final long prev;
    public final long next;

    private WriteIntentListNode(RowId rowId, long link, long prev, long next) {
        this.rowId = rowId;
        this.link = link;
        this.prev = prev;
        this.next = next;
    }

    public static WriteIntentListNode createHeadNode(@NotNull RowId rowId, long link, long prev) {
        return createNode(rowId, link, prev, 0);
    }

    public static WriteIntentListNode createNode(@NotNull RowId rowId, long link, long prev, long next) {
        return new WriteIntentListNode(rowId, link, prev, next);
    }

    @Override
    public int hashCode() {
        return rowId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        WriteIntentListNode other = (WriteIntentListNode) obj;

        return rowId.equals(other.rowId);
    }
}
