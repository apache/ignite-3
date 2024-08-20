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

package org.apache.ignite.internal.tx;

import java.io.Serializable;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Result for both update and update all commands.
 */
public class UpdateCommandResult implements Serializable {
    private static final long serialVersionUID = 2213057546590681613L;

    private final boolean primaryReplicaMatch;

    /** Should be {@code null} if {@link #primaryReplicaMatch} is {@code true}. */
    @Nullable
    private final Long currentLeaseStartTime;

    /** {@code true} if primary replica belongs to the raft group topology, (@code false) otherwise. */
    private final boolean primaryInPeersAndLearners;

    /**
     * Constructor.
     *
     * @param primaryReplicaMatch Whether the command should be successfully applied on primary replica.
     */
    public UpdateCommandResult(boolean primaryReplicaMatch, boolean primaryInPeersAndLearners) {
        this(primaryReplicaMatch, null, primaryInPeersAndLearners);
    }

    // TODO sanpwc javadoc
    /**
     * Constructor.
     *
     * @param primaryReplicaMatch Whether the command should be successfully applied on primary replica.
     * @param currentLeaseStartTime Actual lease start time.
     */
    public UpdateCommandResult(
            boolean primaryReplicaMatch,
            @Nullable Long currentLeaseStartTime,
            boolean primaryInPeersAndLearners
    ) {
        assert primaryReplicaMatch || currentLeaseStartTime != null : "Incorrect UpdateCommandResult.";

        this.primaryReplicaMatch = primaryReplicaMatch;
        this.currentLeaseStartTime = currentLeaseStartTime;
        this.primaryInPeersAndLearners = primaryInPeersAndLearners;
    }

    /**
     * Whether the command should be successfully applied on primary replica.
     *
     * @return Whether the command should be successfully applied on primary replica.
     */
    public boolean isPrimaryReplicaMatch() {
        return primaryReplicaMatch;
    }

    /**
     * Should be {@code null} if {@link #primaryReplicaMatch} is {@code true}.
     *
     * @return Actual lease start time.
     */
    @Nullable
    public Long currentLeaseStartTime() {
        return currentLeaseStartTime;
    }

    // TODO sanpwc javadoc.
    public boolean isPrimaryInPeersAndLearners() {
        return primaryInPeersAndLearners;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UpdateCommandResult that = (UpdateCommandResult) o;
        return primaryReplicaMatch == that.primaryReplicaMatch && primaryInPeersAndLearners == that.primaryInPeersAndLearners
                && Objects.equals(currentLeaseStartTime, that.currentLeaseStartTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryReplicaMatch, currentLeaseStartTime, primaryInPeersAndLearners);
    }
}
