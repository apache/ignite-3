/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.cluster.management;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

/**
 * Local in-memory state of a {@link ClusterManagementGroupManager}.
 */
class InMemoryState {
    /** Node names that host the Meta Storage. */
    private final Collection<String> metaStorageNodes;

    /** Validation token obtained after successful remote validation on the CMG leader. */
    private final UUID validationToken;

    InMemoryState(Collection<String> metaStorageNodes, UUID validationToken) {
        this.metaStorageNodes = Set.copyOf(metaStorageNodes);
        this.validationToken = validationToken;
    }

    Collection<String> metaStorageNodes() {
        return metaStorageNodes;
    }

    UUID validationToken() {
        return validationToken;
    }
}
