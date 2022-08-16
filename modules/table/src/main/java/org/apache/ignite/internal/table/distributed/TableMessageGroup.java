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

package org.apache.ignite.internal.table.distributed;

import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message group for the table module.
 */
@MessageGroup(groupType = 9, groupName = "TableMessages")
public class TableMessageGroup {
    /**
     * Message type for {@link org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest}.
     */
    public static final short RW_SINGLE_ROW_REPLICA_REQUEST = 0;

    /**
     * Message type for {@link org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowReplicaRequest}.
     */
    public static final short RW_MULTI_ROW_REPLICA_REQUEST = 1;

    /**
     * Message type for {@link org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSwapRowReplicaRequest}.
     */
    public static final short RW_DUAL_ROW_REPLICA_REQUEST = 2;

    /**
     * Message type for {@link org.apache.ignite.internal.table.distributed.replication.request.ScanRetrieveBatchReplicaRequest}.
     */
    public static final short RW_SCAN_RETRIEVE_BATCH_REPLICA_REQUEST = 3;

    /**
     * Message type for {@link org.apache.ignite.internal.table.distributed.replication.request.ScanCloseReplicaRequest}.
     */
    public static final short RW_SCAN_CLOSE_REPLICA_REQUEST = 4;
}
