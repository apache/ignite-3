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

package org.apache.ignite.internal.rest.api.cluster;

/**
 * Enum of possible group statuses.
 */
public enum GroupStatus {
    /**
     * The group is completely healthy. Minor losses in any of the groups are possible,
     * but this does not affect the operation of the cluster.
     */
    HEALTHY,

    /**
     * The group has lost its majority and is not healthy.
     * To restore their operation, it is necessary to restore the majority to the group.
     */
    MAJORITY_LOST,
}
