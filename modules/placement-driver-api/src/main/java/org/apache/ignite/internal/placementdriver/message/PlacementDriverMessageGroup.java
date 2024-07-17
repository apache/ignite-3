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

package org.apache.ignite.internal.placementdriver.message;

import static org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup.GROUP_NAME;
import static org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup.GROUP_TYPE;

import org.apache.ignite.internal.network.annotations.MessageGroup;

/**
 * Message group for placement driver messages.
 */
@MessageGroup(groupType = GROUP_TYPE, groupName = GROUP_NAME)
public interface PlacementDriverMessageGroup {
    /** Placement driver message group type. */
    short GROUP_TYPE = 11;

    String GROUP_NAME = "PlacementDriverMessages";

    short LEASE_GRANTED_MESSAGE = 0;

    short LEASE_GRANTED_MESSAGE_RESPONSE = 1;

    short STOP_LEASE_PROLONGATION = 2;
}
