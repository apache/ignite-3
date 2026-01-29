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

package org.apache.ignite.internal.partition.replicator.schemacompat;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.io.Serializable;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Result of a schema compatibility validation.
 */
public class CompatibilityValidationResult implements Serializable {
    private static final long serialVersionUID = 1924417766843882431L;

    private static final CompatibilityValidationResult SUCCESSFUL_RESULT = new CompatibilityValidationResult(
            ValidationStatus.SUCCESS,
            "",
            -1,
            -1,
            null
    );

    private enum ValidationStatus {
        SUCCESS, INCOMPATIBLE_CHANGE, TABLE_DROPPED
    }

    private final ValidationStatus status;
    private final String failedTableName;
    private final int fromSchemaVersion;
    private final @Nullable Integer toSchemaVersion;
    private final @Nullable String details;

    /**
     * Returns a successful validation result.
     *
     * @return A successful validation result.
     */
    public static CompatibilityValidationResult success() {
        return SUCCESSFUL_RESULT;
    }

    /**
     * Creates a validation result denoting incompatible schema change.
     *
     * @param failedTableName Table which schema change is incompatible.
     * @param fromSchemaVersion Version number of the schema from which an incompatible transition tried to be made.
     * @param toSchemaVersion Version number of the schema to which an incompatible transition tried to be made.
     * @return A validation result for a failure.
     */
    public static CompatibilityValidationResult incompatibleChange(
            String failedTableName,
            int fromSchemaVersion,
            int toSchemaVersion,
            @Nullable String details
    ) {
        return new CompatibilityValidationResult(
                ValidationStatus.INCOMPATIBLE_CHANGE,
                failedTableName,
                fromSchemaVersion,
                toSchemaVersion,
                details
        );
    }

    /**
     * Creates a validation result denoting 'table already dropped when commit is made' situation.
     *
     * @param failedTableName Table which schema change is incompatible.
     * @param fromSchemaVersion Version number of the schema from which an incompatible transition tried to be made.
     * @return A validation result for a failure.
     */
    public static CompatibilityValidationResult tableDropped(String failedTableName, int fromSchemaVersion) {
        return new CompatibilityValidationResult(ValidationStatus.TABLE_DROPPED, failedTableName, fromSchemaVersion, null, null);
    }

    private CompatibilityValidationResult(
            ValidationStatus status,
            String failedTableName,
            int fromSchemaVersion,
            @Nullable Integer toSchemaVersion,
            @Nullable String details
    ) {
        this.status = status;
        this.failedTableName = failedTableName;
        this.fromSchemaVersion = fromSchemaVersion;
        this.toSchemaVersion = toSchemaVersion;
        this.details = details;
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
     * Returns name of the table for which the validation has failed. Should only be called for a failed validation result, otherwise an
     * exception is thrown.
     *
     * @return Table name.
     */
    public String failedTableName() {
        assert !isSuccessful() : "Should not be called on a successful result";

        return failedTableName;
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

    /**
     * For failed validation returns details why the operation failed.
     *
     * @return Details why the operation failed.
     */
    public String details() {
        assert !isSuccessful() : "Should not be called on a successful result";
        assert details != null : "Should not be called when there are no details";

        return details;
    }

    /**
     * Throws an exception produced by the given factory if the validation failed.
     *
     * @param exceptionFactory Exception factory.
     */
    public <X extends Exception> void throwIfSchemaValidationOnCommitFailed(Function<String, ? extends X> exceptionFactory) throws X {
        CompatibilityValidationResult validationResult = this;

        if (!isSuccessful()) {
            if (isTableDropped()) {
                throw exceptionFactory.apply(
                        format("Commit failed because a table was already dropped [table={}]", validationResult.failedTableName())
                );
            } else {
                throw exceptionFactory.apply(
                        format(
                                "Commit failed because schema is not forward-compatible "
                                        + "[fromSchemaVersion={}, toSchemaVersion={}, table={}, details={}]",
                                validationResult.fromSchemaVersion(),
                                validationResult.toSchemaVersion(),
                                validationResult.failedTableName(),
                                validationResult.details()
                        )
                );
            }
        }
    }
}
