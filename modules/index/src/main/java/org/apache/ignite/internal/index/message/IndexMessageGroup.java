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

package org.apache.ignite.internal.index.message;

import org.apache.ignite.internal.network.annotations.MessageGroup;

/** Message types for index module. */
@MessageGroup(groupType = 12, groupName = "IndexMessages")
public class IndexMessageGroup {
    /** Message type for {@link IsNodeFinishedRwTransactionsStartedBeforeRequest}. */
    public static final short IS_NODE_FINISHED_RW_TRANSACTIONS_STARTED_BEFORE_REQUEST = 0;

    /** Message type for {@link IsNodeFinishedRwTransactionsStartedBeforeResponse}. */
    public static final short IS_NODE_FINISHED_RW_TRANSACTIONS_STARTED_BEFORE_RESPONSE = 1;
}
