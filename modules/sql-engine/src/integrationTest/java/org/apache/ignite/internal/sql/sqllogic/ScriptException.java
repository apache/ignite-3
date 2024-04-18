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


package org.apache.ignite.internal.sql.sqllogic;

import org.jetbrains.annotations.Nullable;

/**
 * An exception with position in a script.
 */
final class ScriptException extends RuntimeException {

    private static final long serialVersionUID = -5972259903486232854L;

    /**
     * Creates an exception with a message in the one of the following forms.
     * <ul>
     *     <li>When a message parameter is specified and is not empty: {@code error at position: message}</li>
     *     <li>When a message parameter is omitted: {@code error at position}</li>
     * </ul>
     *
     * @param error an error.
     * @param position a position in a script where a problem has occurred.
     * @param message an additional message.
     */
    ScriptException(String error, ScriptPosition position, @Nullable String message) {
        super(scriptErrorMessage(error, position, message));
    }

    /**
     * Similar to {@link ScriptException#ScriptException(String, ScriptPosition, String)} but allow to pass a cause.
     *
     * @param error an error.
     * @param position a position in a script where a problem has occurred.
     * @param message an additional message.
     * @param cause a cause of this exception.
     */
    ScriptException(String error, ScriptPosition position, @Nullable String message, @Nullable Throwable cause) {
        super(scriptErrorMessage(error, position, message), cause);
    }

    private static String scriptErrorMessage(String message, ScriptPosition position, @Nullable String details) {
        if (details == null || details.isEmpty()) {
            return String.format("%s at %s", message, position);
        } else {
            return String.format("%s at %s: %s", message, position, details);
        }
    }
}
