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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Cursor of values at the given timestamp.
 * <p/>
 * This cursor iterates over values from the storage as they were at the given timestamp.
 * <p/>
 * If there is a row with specified row id and timestamp - return it.
 * If there are multiple versions of row with specified row id:
 * <ol>
 *     <li>If there is only write-intent - return write-intent.</li>
 *     <li>If there is write-intent and previous commit is older than timestamp - return write-intent.</li>
 *     <li>If there is a commit older than timestamp, but no write-intent - return said commit.</li>
 *     <li>If there are two commits one older and one newer than timestamp - return older commit.</li>
 *     <li>There are commits but they're all newer than timestamp - return nothing.</li>
 * </ol>
 * <p/>
 * In case if write-intent was returned, committed row may be obtained via {@link #committed(HybridTimestamp)} method
 * using {@link ReadResult#newestCommitTimestamp()}.
 */
public interface PartitionTimestampCursor extends Cursor<ReadResult> {
    /**
     * Returns a committed row within the current row id that is associated with the given timestamp.
     *
     * @param timestamp Commit timestamp.
     * @return Row or {@code null} if there is no row for the given timestamp.
     */
    @Nullable BinaryRow committed(HybridTimestamp timestamp);
}
