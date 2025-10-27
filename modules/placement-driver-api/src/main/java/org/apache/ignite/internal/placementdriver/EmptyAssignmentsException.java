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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.PlacementDriver.EMPTY_ASSIGNMENTS_ERR;

import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Exception thrown when there are no assignments available.
 */
public class EmptyAssignmentsException extends IgniteException {
    private static final long serialVersionUID = 1698246028174494488L;

    private final ReplicationGroupId groupId;

    /**
     * Constructor.
     *
     * @param groupId Replication group id.
     * @param cause Optional cause.
     */
    public EmptyAssignmentsException(ReplicationGroupId groupId, @Nullable Throwable cause) {
        super(EMPTY_ASSIGNMENTS_ERR, format("Empty assignments for group [groupId={}].", groupId), cause);

        this.groupId = groupId;
    }

    /**
     * Gets replication group id.
     *
     * @return Replication group id.
     */
    public ReplicationGroupId groupId() {
        return groupId;
    }
}
