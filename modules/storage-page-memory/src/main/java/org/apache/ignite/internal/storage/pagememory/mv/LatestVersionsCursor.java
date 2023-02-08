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

import org.apache.ignite.internal.storage.ReadResult;

/**
 * Implementation of the cursor that iterates over the page memory storage with the respect to the transaction id. Scans the partition
 * and returns a cursor of values. All filtered values must either be uncommitted in the current transaction or already committed in a
 * different transaction.
 */
class LatestVersionsCursor extends AbstractPartitionTimestampCursor {
    LatestVersionsCursor(AbstractPageMemoryMvPartitionStorage storage) {
        super(storage);
    }

    @Override
    ReadResult findRowVersion(VersionChain versionChain) {
        return storage.findLatestRowVersion(versionChain);
    }
}
