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

package org.apache.ignite.internal.table.message;

import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Table module message group.
 */
@MessageGroup(groupType = 8, groupName = "TableMessages")
public interface TableMessageGroup {
    /** {@link HasDataRequest}. */
    int HAS_DATA_REQUEST = 0;

    /** {@link HasDataResponse}. */
    int HAS_DATA_RESPONSE = 1;

    /** {@link SnapshotMetaRequest}. */
    int SNAPSHOT_META_REQUEST = 10;

    /** {@link SnapshotMetaResponse}. */
    int SNAPSHOT_META_RESPONSE = 11;

    /** {@link SnapshotMvDataRequest}. */
    int SNAPSHOT_MV_DATA_REQUEST = 12;

    /** {@link SnapshotMvDataResponse.SnapshotMvDataSingleResponse}. */
    int SNAPSHOT_MV_DATA_SINGLE_RESPONSE = 13;

    /** {@link SnapshotMvDataResponse}. */
    int SNAPSHOT_MV_DATA_RESPONSE = 14;

    /** {@link SnapshotTxDataRequest}. */
    int SNAPSHOT_TX_DATA_REQUEST = 15;

    /** {@link SnapshotTxDataResponse}. */
    int SNAPSHOT_TX_DATA_RESPONSE = 16;
}
