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

package org.apache.ignite.internal.placementdriver;

import java.util.function.Function;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.jetbrains.annotations.Nullable;

/**
 * Utils for placement driver.
 */
public class Utils {
    /**
     * Extracts zone ID from the given group id.
     *
     * @param groupId Replication group ID.
     * @param colocationEnabled Whether colocation is enabled.
     * @param zoneIdByTableIdResolver Function to resolve zone ID by table ID if colocation is disabled.
     * @return Zone ID or {@code null}.
     */
    @Nullable
    public static Integer extractZoneIdFromGroupId(
            ReplicationGroupId groupId,
            boolean colocationEnabled,
            Function<Integer, Integer> zoneIdByTableIdResolver
    ) {
        assert groupId instanceof ZonePartitionId : "Unexpected replication group id type ["
                + "group=" + groupId
                + ", type=" + groupId.getClass().getSimpleName()
                + ", colocationEnabled=" + colocationEnabled
                + ']';

        if (colocationEnabled && groupId instanceof ZonePartitionId) {
            return ((ZonePartitionId) groupId).zoneId();
        } else {
            return zoneIdByTableIdResolver.apply(((TablePartitionId) groupId).tableId());
        }
    }
}
