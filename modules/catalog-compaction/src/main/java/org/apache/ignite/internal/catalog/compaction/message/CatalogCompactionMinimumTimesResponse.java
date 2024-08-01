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

package org.apache.ignite.internal.catalog.compaction.message;

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Response message containing timestamps from the entire cluster required to safely perform catalog compaction.
 *
 * <p>This includes the following:
 * <ol>
 *     <li>the minimum timestamp to which, from the local node's perspective, the catalog history can be safely truncated</li>
 *     <li>the minimum starting time among locally started active RW transactions</li>
 * </ol>
 */
@Transferable(CatalogCompactionMessageGroup.MINIMUM_REQUIRED_TIME_RESPONSE)
public interface CatalogCompactionMinimumTimesResponse extends NetworkMessage {
    /** Returns node's minimum required time. */
    long minimumRequiredTime();

    /** Returns node's minimum starting time among locally started active RW transactions. */
    long minimumActiveTxTime();
}
