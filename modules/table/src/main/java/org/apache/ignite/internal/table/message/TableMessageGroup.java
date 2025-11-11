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

import org.apache.ignite.internal.network.annotations.MessageGroup;

/**
 * Message group for table module.
 */
@MessageGroup(groupType = TableMessageGroup.GROUP_TYPE, groupName = "TableMessages")
public interface TableMessageGroup {
    /** Table message group type. */
    short GROUP_TYPE = 17;

    /** Message type for {@link GetEstimatedSizeWithLastModifiedTsRequest}. */
    short GET_ESTIMATED_SIZE_WITH_MODIFIED_TS_MESSAGE_REQUEST = 1;

    /** Message type for {@link GetEstimatedSizeWithLastModifiedTsResponse}. */
    short GET_ESTIMATED_SIZE_WITH_MODIFIED_TS_MESSAGE_RESPONSE = 2;

    /** Message type for {@link PartitionModificationInfoMessage}. */
    short GET_ESTIMATED_SIZE_WITH_MODIFIED_TS_MESSAGE = 3;
}
