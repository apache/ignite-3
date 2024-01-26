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

package org.apache.ignite.internal.table.distributed.replicator;

import org.jetbrains.annotations.Nullable;

/**
 * Result of a schema compatibility validation.
 */
public class CompatValidationResult {
    private static final CompatValidationResult SUCCESSFUL_RESULT = new CompatValidationResult(ValidationStatus.SUCCESS, -1, -1, -1);

    private enum ValidationStatus {
        SUCCESS, INCOMPATIBLE_CHANGE, TABLE_DROPPED
    }

    private final ValidationStatus status;
    private final int failedTableId;
    private final int fromSchemaVersion;
    private final @Nullable Integer toSchemaVersion;

    /**
     * Returns a successful validation result.
     *
     * @return A successful validation result.
     */
    public static CompatValidationResult success() {
        return SUCCESSFUL_RESULT;
    }

    /**
     * Creates a validation result denoting incompatible schema change.
     *
     * @param failedTableId Table which schema change is incompatible.
     * @param fromSchemaVersion Version number of the schema from which an incompatible transition tried to be made.
     * @param toSchemaVersion Version number of the schema to which an incompatible transition tried to be made.
     * @return A validation result for a failure.
     */
    public static CompatValidationResult incompatibleChange(int failedTableId, int fromSchemaVersion, int toSchemaVersion) {
        return new CompatValidationResult(ValidationStatus.INCOMPATIBLE_CHANGE, failedTableId, fromSchemaVersion, toSchemaVersion);
    }

    /**
     * Creates a validation result denoting 'table already dropped when commit is made' situation.
     *
     * @param failedTableId Table which schema change is incompatible.
     * @param fromSchemaVersion Version number of the schema from which an incompatible transition tried to be made.
     * @return A validation result for a failure.
     */
    public static CompatValidationResult tableDropped(int failedTableId, int fromSchemaVersion) {
        return new CompatValidationResult(ValidationStatus.TABLE_DROPPED, failedTableId, fromSchemaVersion, null);
    }

    private CompatValidationResult(
            ValidationStatus status,
            int failedTableId,
            int fromSchemaVersion,
            @Nullable Integer toSchemaVersion
    ) {
        this.status = status;
        this.failedTableId = failedTableId;
        this.fromSchemaVersion = fromSchemaVersion;
        this.toSchemaVersion = toSchemaVersion;
    }

    /**
     * Returns whether the validation was successful.
     *
     * @return Whether the validation was successful
     */
    public boolean isSuccessful() {
        return status == ValidationStatus.SUCCESS;
    }

    /**
     * Returns whether the validation has failed due to table was already dropped at the commit timestamp.
     */
    public boolean isTableDropped() {
        return status == ValidationStatus.TABLE_DROPPED;
    }

    /**
     * Returns ID of the table for which the validation has failed. Should only be called for a failed validation result, otherwise an
     * exception is thrown.
     *
     * @return Table ID.
     */
    public int failedTableId() {
        assert !isSuccessful() : "Should not be called on a successful result";

        return failedTableId;
    }

    /**
     * Returns version number of the schema from which an incompatible transition tried to be made.
     *
     * @return Version number of the schema from which an incompatible transition tried to be made.
     */
    public int fromSchemaVersion() {
        assert !isSuccessful() : "Should not be called on a successful result";

        return fromSchemaVersion;
    }

    /**
     * Returns version number of the schema to which an incompatible transition tried to be made.
     *
     * @return Version number of the schema to which an incompatible transition tried to be made.
     */
    public int toSchemaVersion() {
        assert !isSuccessful() : "Should not be called on a successful result";
        assert toSchemaVersion != null : "Should not be called when there is no toSchemaVersion";

        return toSchemaVersion;
    }
}
