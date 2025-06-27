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

package org.apache.ignite.internal.disaster.system.message;

import org.apache.ignite.internal.network.annotations.MessageGroup;

/**
 * Message Group for disaster recovery of system groups.
 */
@MessageGroup(groupType = 15, groupName = "SystemDisasterRecoveryMessages")
public class SystemDisasterRecoveryMessageGroup {
    /**
     * Message type for {@link ResetClusterMessage}.
     */
    public static final short RESET_CLUSTER = 1;

    /**
     * Message type for {@link StartMetastorageRepairRequest}.
     */
    public static final short METASTORAGE_INDEX_TERM_REQUEST = 2;

    /**
     * Message type for {@link StartMetastorageRepairResponse}.
     */
    public static final short METASTORAGE_INDEX_TERM_RESPONSE = 3;

    /**
     * Message type for {@link BecomeMetastorageLeaderMessage}.
     */
    public static final short BECOME_METASTORAGE_LEADER = 4;
}
