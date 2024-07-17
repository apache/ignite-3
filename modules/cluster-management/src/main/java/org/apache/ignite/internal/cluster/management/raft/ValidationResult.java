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

package org.apache.ignite.internal.cluster.management.raft;

import org.jetbrains.annotations.Nullable;

/**
 * Result of validating a node by the {@link ValidationManager}.
 */
public class ValidationResult {
    @Nullable
    private final String errorDescription;

    private ValidationResult(@Nullable String errorDescription) {
        this.errorDescription = errorDescription;
    }

    /**
     * Creates a successful validation result.
     */
    public static ValidationResult successfulResult() {
        return new ValidationResult(null);
    }

    /**
     * Creates a failed validation result.
     */
    public static ValidationResult errorResult(String errorDescription) {
        return new ValidationResult(errorDescription);
    }

    /**
     * Returns {@code true} if the validation result is successful, {@code false} otherwise.
     */
    boolean isValid() {
        return errorDescription == null;
    }

    /**
     * Returns the validation error description if this result is not successful.
     */
    String errorDescription() {
        assert errorDescription != null;

        return errorDescription;
    }
}
