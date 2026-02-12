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
import org.apache.ignite.internal.partition.replicator.schemacompat.CompatibilityValidationResult;
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

    /** {@code true} if primary replica belongs to the raft group topology: peers and learners, (@code false) otherwise. */
    private final boolean primaryInPeersAndLearners;

    /** The safe timestamp. */
    private final long safeTimestamp;

    /** Result of compatibility validation. Only present if the command was full and validation failed. */
    @Nullable
    private final CompatibilityValidationResult compatibilityValidationResult;

    /**
     * Constructor.
     *
     * @param primaryReplicaMatch Whether the command was executed successfully or failed due to mismatch of primary replica information.
     */
    public UpdateCommandResult(boolean primaryReplicaMatch, boolean primaryInPeersAndLearners, long safeTimestamp) {
        this(primaryReplicaMatch, primaryInPeersAndLearners, safeTimestamp, null);
    }

    /**
     * Constructor.
     *
     * @param primaryReplicaMatch Whether the command was executed successfully or failed due to mismatch of primary replica information.
     */
    public UpdateCommandResult(
            boolean primaryReplicaMatch,
            boolean primaryInPeersAndLearners,
            long safeTimestamp,
            @Nullable CompatibilityValidationResult compatibilityValidationResult
    ) {
        this(primaryReplicaMatch, null, primaryInPeersAndLearners, safeTimestamp, compatibilityValidationResult);
    }

    /**
     * Constructor.
     *
     * @param primaryReplicaMatch Whether the command was executed successfully or failed due to mismatch of primary replica information.
     * @param currentLeaseStartTime Actual lease start time.
     * @param primaryInPeersAndLearners {@code true} if primary replica belongs to the raft group topology: peers and learners,
     *     (@code false) otherwise.
     * @param safeTimestamp The safe timestamp.
     */
    public UpdateCommandResult(
            boolean primaryReplicaMatch,
            @Nullable Long currentLeaseStartTime,
            boolean primaryInPeersAndLearners,
            long safeTimestamp
    ) {
        this(primaryReplicaMatch, currentLeaseStartTime, primaryInPeersAndLearners, safeTimestamp, null);
    }

    private UpdateCommandResult(
            boolean primaryReplicaMatch,
            @Nullable Long currentLeaseStartTime,
            boolean primaryInPeersAndLearners,
            long safeTimestamp,
            @Nullable CompatibilityValidationResult compatibilityValidationResult
    ) {
        assert compatibilityValidationResult != null || primaryReplicaMatch || currentLeaseStartTime != null
                : "Incorrect UpdateCommandResult.";

        this.primaryReplicaMatch = primaryReplicaMatch;
        this.currentLeaseStartTime = currentLeaseStartTime;
        this.primaryInPeersAndLearners = primaryInPeersAndLearners;
        this.safeTimestamp = safeTimestamp;
        this.compatibilityValidationResult = compatibilityValidationResult;
    }

    /**
     * Whether the command was executed successfully or failed due to mismatch of primary replica information, i.e. lease start time that
     * was sent along with the command doesn't match the one in raft updated by handlePrimaryReplicaChangeCommand.
     *
     * @return Whether the command was executed successfully or failed due to mismatch of primary replica information.
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

    /**
     * Returns whether primary replica belongs to the raft group topology: peers and learners.
     *
     * @return {@code true} if primary replica belongs to the raft group topology: peers and learners, (@code false) otherwise.
     */
    public boolean isPrimaryInPeersAndLearners() {
        return primaryInPeersAndLearners;
    }

    /**
     * Returns a safe timestamp associated with the moment of command application.
     *
     * @return The timestamp.
     */
    public long safeTimestamp() {
        return safeTimestamp;
    }

    /**
     * Returns result of compatibility validation. Only present if the command was full and validation failed.
     */
    @Nullable
    public CompatibilityValidationResult compatibilityValidationResult() {
        return compatibilityValidationResult;
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
                && Objects.equals(currentLeaseStartTime, that.currentLeaseStartTime)
                && Objects.equals(compatibilityValidationResult, that.compatibilityValidationResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryReplicaMatch, currentLeaseStartTime, primaryInPeersAndLearners, compatibilityValidationResult);
    }
}
