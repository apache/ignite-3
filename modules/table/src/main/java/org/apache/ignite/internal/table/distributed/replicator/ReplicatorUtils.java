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

package org.apache.ignite.internal.table.distributed.replicator;

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.tx.TransactionIds;

/** Auxiliary class. */
class ReplicatorUtils {

    /**
     * Extracts begin timestamp of a read-write transaction from a request.
     *
     * @param request Read-write replica request.
     */
    static HybridTimestamp beginRwTxTs(ReadWriteReplicaRequest request) {
        return TransactionIds.beginTimestamp(request.transactionId());
    }

    /**
     * Returns the active catalog version by begin timestamp of a read-write transaction from a request.
     *
     * @param catalogService Catalog service.
     * @param request Read-write replica request.
     */
    static int rwTxActiveCatalogVersion(CatalogService catalogService, ReadWriteReplicaRequest request) {
        HybridTimestamp beginRwTxTs = beginRwTxTs(request);

        return catalogService.activeCatalogVersion(beginRwTxTs.longValue());
    }
}
