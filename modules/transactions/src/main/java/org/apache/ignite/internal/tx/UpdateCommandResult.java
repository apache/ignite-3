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

    /**
     * Constructor.
     *
     * @param primaryReplicaMatch Whether the command should be successfully applied on primary replica.
     */
    public UpdateCommandResult(boolean primaryReplicaMatch) {
        this(primaryReplicaMatch, null);
    }

    /**
     * Constructor.
     *
     * @param primaryReplicaMatch Whether the command should be successfully applied on primary replica.
     * @param currentLeaseStartTime Actual lease start time.
     */
    public UpdateCommandResult(boolean primaryReplicaMatch, @Nullable Long currentLeaseStartTime) {
        assert primaryReplicaMatch || currentLeaseStartTime != null : "Incorrect UpdateCommandResult.";

        this.primaryReplicaMatch = primaryReplicaMatch;
        this.currentLeaseStartTime = currentLeaseStartTime;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UpdateCommandResult that = (UpdateCommandResult) o;

        if (primaryReplicaMatch != that.primaryReplicaMatch) {
            return false;
        }
        return currentLeaseStartTime != null ? currentLeaseStartTime.equals(that.currentLeaseStartTime)
                : that.currentLeaseStartTime == null;
    }

    @Override
    public int hashCode() {
        int result = (primaryReplicaMatch ? 1 : 0);
        result = 31 * result + (currentLeaseStartTime != null ? currentLeaseStartTime.hashCode() : 0);
        return result;
    }
}
