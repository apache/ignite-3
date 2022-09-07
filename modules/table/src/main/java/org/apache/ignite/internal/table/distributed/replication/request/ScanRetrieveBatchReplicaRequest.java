/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table.distributed.replication.request;

import java.util.function.Predicate;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.network.annotations.Marshallable;

/**
 * Scan retrieve batch replica request.
 */
public interface ScanRetrieveBatchReplicaRequest extends ReplicaRequest {
    /** Batch size. */
    int batchSize();

    /** The id uniquely determines a cursor for the transaction. */
    long scanId();

    /**
     * Gets a scan row filter. The filter has a sense only for first request, for the second one and followings the field is ignored.
     *
     * @return Row filter predicate.
     */
    @Marshallable
    Predicate<BinaryRow> rowFilter();
}
