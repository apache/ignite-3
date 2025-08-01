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

package org.apache.ignite.internal.metastorage.server;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * {@link SimpleInMemoryKeyValueStorage} container for creating and restoring from a snapshot.
 *
 * @see SimpleInMemoryKeyValueStorage#snapshot(Path)
 * @see SimpleInMemoryKeyValueStorage#restoreSnapshot(Path)
 */
class SimpleInMemoryKeyValueStorageSnapshot implements Serializable {
    private static final long serialVersionUID = -4263291644707737634L;

    static final String FILE_NAME = "snapshot.bin";

    final Map<byte[], List<Long>> keysIdx;

    final Map<Long, Long> tsToRevMap;

    final Map<Long, HybridTimestamp> revToTsMap;

    final Map<Long, Map<byte[], ValueSnapshot>> revsIdx;

    final long rev;

    final long savedCompactionRevision;

    final long term;

    final long index;

    SimpleInMemoryKeyValueStorageSnapshot(
            Map<byte[], List<Long>> keysIdx,
            Map<Long, Long> tsToRevMap,
            Map<Long, HybridTimestamp> revToTsMap,
            Map<Long, Map<byte[], ValueSnapshot>> revsIdx,
            long rev,
            long savedCompactionRevision,
            long term,
            long index
    ) {
        this.keysIdx = keysIdx;
        this.tsToRevMap = tsToRevMap;
        this.revToTsMap = revToTsMap;
        this.revsIdx = revsIdx;
        this.rev = rev;
        this.savedCompactionRevision = savedCompactionRevision;
        this.term = term;
        this.index = index;
    }
}
