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

import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;

/**
 * Operations that can be performed on a row version. Used to abstract different behaviors for different row version types.
 */
interface RowVersionOperations {
    /**
     * Removes this row version from the write intents list if it's part of it.
     *
     * @param storage The partition storage.
     * @param operationInfoSupplier Supplier of additional info about the operation for logging purposes.
     */
    void removeFromWriteIntentsList(
            AbstractPageMemoryMvPartitionStorage storage,
            Supplier<String> operationInfoSupplier
    );

    /**
     * Returns the link to the next write intent's row version if the current row version contains this information,
     * or the fallback link if it doesn't.
     *
     * @param fallbackLink Link to return if the current row version doesn't support the write intent list links.
     */
    long nextWriteIntentLink(long fallbackLink);

    /**
     * Returns the link to the previous write intent's row version if the current row version contains this information,
     * or {@link PageIdUtils#NULL_LINK} if it doesn't.
     */
    long prevWriteIntentLink();

    /**
     * Returns a page handler that can convert this write intent to its committed version in-place.
     */
    PageHandler<HybridTimestamp, Object> converterToCommittedVersion();
}
