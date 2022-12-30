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

/**
 * Provides error messages for validation errors returned by {@link IgniteSqlValidator}.
 */
final class IgniteSqlValidatorErrorMessages {

    private static final String ORIGINAL_COLUMN_NOT_NULLABLE_MESSAGE = "has no default value and does not allow NULLs";

    private static final String IGNITE_COLUMN_NOT_NULLABLE_MESSAGE = "does not allow NULLs";

    private IgniteSqlValidatorErrorMessages() {
    }

    /**
     * Returns an apache ignite specific error message for the given {@link CalciteContextException} or returns
     * the original message from the given exception.
     *
     * @param e an exception.
     * @return  an error message.
     */
    static String resolveErrorMessage(CalciteContextException e) {
        var message = e.getMessage();
        if (message != null) {
            message = message.replace(ORIGINAL_COLUMN_NOT_NULLABLE_MESSAGE,
                    IGNITE_COLUMN_NOT_NULLABLE_MESSAGE);
        }
        return message;
    }
}
