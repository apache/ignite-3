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

import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Storage used by the cluster reset tools.
 */
public interface ClusterResetStorage extends MetastorageRepairStorage {
    /**
     * Reads {@link ResetClusterMessage} from the persistent state; returns {@code null} if it's not saved.
     */
    @Nullable ResetClusterMessage readResetClusterMessage();

    /**
     * Removes saved {@link ResetClusterMessage}.
     */
    void removeResetClusterMessage();

    /**
     * Saves a {@link ResetClusterMessage} to the volatile state.
     *
     * @param message Message to save.
     */
    void saveVolatileResetClusterMessage(ResetClusterMessage message);
}
