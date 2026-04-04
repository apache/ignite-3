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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Not thread-safe memtable implementation designed to be used by a single thread.
 */
class SingleThreadMemTable extends AbstractMemTable {
    private final Map<Long, SegmentInfo> memtable = new HashMap<>();

    @Override
    protected Map<Long, SegmentInfo> memtable(long groupId) {
        return memtable;
    }

    @Override
    public Iterator<Entry<Long, SegmentInfo>> iterator() {
        return memtable.entrySet().iterator();
    }

    @Override
    public int numGroups() {
        return memtable.size();
    }
}
