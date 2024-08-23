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
 * Message is used to propagate the minimum begin tx time on replicas.
 *
 * <p>The whole process consists of the following steps:
 * <ul>
 *     <li>Collect local minimum starting time among logical nodes (see {@link CatalogCompactionMinimumTimesRequest})</li>
 *     <li>Compute global minimum</li>
 *     <li>Send global minimum to all logical nodes (this message)</li>
 *     <li>Each node updates the replication groups for which that node is the primary</li>
 * </ul>
 */
@Transferable(CatalogCompactionMessageGroup.PREPARE_TO_UPDATE_TIME_ON_REPLICAS_MESSAGE)
public interface CatalogCompactionPrepareUpdateTxBeginTimeMessage extends NetworkMessage {
    /** Returns the minimum starting time among all active RW transactions. */
    long timestamp();
}
