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

package org.apache.ignite.internal.raft.storage.segstore;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Immutable version of an index memtable used by the {@link RaftLogCheckpointer}.
 *
 * @see WriteModeIndexMemTable
 */
interface ReadModeIndexMemTable {
    /**
     * Returns the offset in the segment file where the log entry with the given {@code logIndex} is stored or {@code 0} if the log entry
     * was not found in the memtable.
     */
    int getSegmentFileOffset(long groupId, long logIndex);

    /**
     * Returns an iterator over all {@code Group ID -> SegmentInfo} entries in this memtable.
     */
    Iterator<Entry<Long, SegmentInfo>> iterator();

    /**
     * Returns the number of Raft Group IDs stored in this memtable.
     */
    int numGroups();
}
