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

import java.util.Objects;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Result of a schema compatibility validation.
 */
public class CompatValidationResult {
    private static final CompatValidationResult SUCCESS = new CompatValidationResult(true, null, -1, -1);

    private final boolean ok;
    @Nullable
    private final UUID failedTableId;
    private final int fromSchemaVersion;
    private final int toSchemaVersion;

    /**
     * Returns a successful validation result.
     *
     * @return A successful validation result.
     */
    public static CompatValidationResult success() {
        return SUCCESS;
    }

    /**
     * Creates a validation result denoting validation failure.
     *
     * @param failedTableId Table which schema change is incompatible.
     * @param fromSchemaVersion Version number of the schema from which an incompatible transition tried to be made.
     * @param toSchemaVersion Version number of the schema to which an incompatible transition tried to be made.
     * @return A validation result for a failure.
     */
    public static CompatValidationResult failure(UUID failedTableId, int fromSchemaVersion, int toSchemaVersion) {
        return new CompatValidationResult(false, failedTableId, fromSchemaVersion, toSchemaVersion);
    }

    private CompatValidationResult(boolean ok, @Nullable UUID failedTableId, int fromSchemaVersion, int toSchemaVersion) {
        this.ok = ok;
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
        return ok;
    }

    /**
     * Returns ID of the table for which the validation has failed. Should only be called for a failed validation result,
     * otherwise an exception is thrown.
     *
     * @return Table ID.
     */
    public UUID failedTableId() {
        return Objects.requireNonNull(failedTableId);
    }

    /**
     * Returns version number of the schema from which an incompatible transition tried to be made. Should only be called for a failed
     * validation result, otherwise an exception is thrown.
     *
     * @return Version number of the schema from which an incompatible transition tried to be made.
     */
    public int fromSchemaVersion() {
        throwIfSuccessful();

        return fromSchemaVersion;
    }

    private void throwIfSuccessful() {
        if (ok) {
            throw new IllegalStateException("Should not be called on a successful result");
        }
    }

    /**
     * Returns version number of the schema to which an incompatible transition tried to be made. Should only be called for a failed
     *      * validation result, otherwise an exception is thrown.
     *
     * @return Version number of the schema to which an incompatible transition tried to be made.
     */
    public int toSchemaVersion() {
        throwIfSuccessful();

        return toSchemaVersion;
    }
}
