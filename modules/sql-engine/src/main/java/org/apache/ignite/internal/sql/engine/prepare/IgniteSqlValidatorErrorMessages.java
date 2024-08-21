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

package org.apache.ignite.internal.sql.engine.prepare;

import org.apache.calcite.runtime.CalciteContextException;
import org.jetbrains.annotations.Nullable;

/**
 * Provides error messages for validation errors returned by {@link IgniteSqlValidator}.
 */
public final class IgniteSqlValidatorErrorMessages {
    enum Overrides {
        COLUMN_NOT_NULLABLE(
                "has no default value and does not allow NULLs",
                "does not allow NULLs"
        ),
        NATURAL_OR_USING_COLUMN_NOT_COMPATIBLE(
                "matched using NATURAL keyword or USING clause has incompatible types: cannot compare",
                "matched using NATURAL keyword or USING clause has incompatible types in this context:"
        );


        private final String toReplace;
        private final String replacement;

        Overrides(String toReplace, String replacement) {
            this.toReplace = toReplace;
            this.replacement = replacement;
        }

        boolean test(String originalMessage) {
            return originalMessage.contains(toReplace);
        }

        String replace(String originalMessage) {
            return originalMessage.replace(toReplace, replacement);
        }
    }

    private IgniteSqlValidatorErrorMessages() {
    }

    /**
     * Returns an apache ignite specific error message for the given {@link CalciteContextException} or returns null if original message
     * should be preserved.
     *
     * @param message An original message.
     * @return An updated message.
     */
    public static @Nullable String resolveErrorMessage(@Nullable String message) {
        if (message == null) {
            return null;
        }

        for (Overrides override : Overrides.values()) {
            if (override.test(message)) {
                return override.replace(message);
            }
        }

        return null;
    }
}
