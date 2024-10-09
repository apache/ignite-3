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

package org.apache.ignite.internal.disaster.system.storage;

import java.util.UUID;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Storage used by the Metastorage repair tools.
 */
public interface MetastorageRepairStorage {
    /**
     * Reads {@link ResetClusterMessage} from the volatile state; returns {@code null} if it's not saved.
     */
    @Nullable ResetClusterMessage readVolatileResetClusterMessage();

    /**
     * Reads cluster ID in which this node witnessed (that is, participated or reentered the Metastorage group if not participated)
     * last Metastorage repair.
     */
    @Nullable UUID readWitnessedMetastorageRepairClusterId();

    /**
     * Saves cluster ID in which this node witnessed (that is, participated or reentered the Metastorage group if not participated)
     * a Metastorage repair.
     *
     * @param repairClusterId ID of the cluster.
     */
    void saveWitnessedMetastorageRepairClusterId(UUID repairClusterId);
}
