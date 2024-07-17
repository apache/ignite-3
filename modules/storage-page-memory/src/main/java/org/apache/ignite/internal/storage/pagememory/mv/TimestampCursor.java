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

package org.apache.ignite.internal.storage.pagememory.mv;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;

/**
 * Implementation of the {@link PartitionTimestampCursor} over the page memory storage. See {@link PartitionTimestampCursor} for the
 * details on the API.
 */
class TimestampCursor extends AbstractPartitionTimestampCursor {
    private final HybridTimestamp timestamp;

    TimestampCursor(
            AbstractPageMemoryMvPartitionStorage storage,
            HybridTimestamp timestamp
    ) {
        super(storage);

        this.timestamp = timestamp;
    }

    @Override
    ReadResult findRowVersion(VersionChain versionChain) {
        return storage.findRowVersionByTimestamp(versionChain, timestamp);
    }
}
