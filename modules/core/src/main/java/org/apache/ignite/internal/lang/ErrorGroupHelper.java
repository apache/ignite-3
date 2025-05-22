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

package org.apache.ignite.internal.lang;

import static org.apache.ignite.lang.ErrorGroup.extractErrorCode;

import java.util.UUID;

/**
 * The class contains just single method which is fully copy of {@link ErrorGroup#errorMessage(String, UUID, String, int, String)} which is
 * placed in public API and shouldn't be visible to outside. It required just to hide our internal things.
 */
class ErrorGroupHelper {

    /**
     * Creates a new error message with predefined prefix. Code of the method should be identical to {@code ErrorGroup.errorMessage}. We
     * have code duplication just to remove from public api our dirty part.
     *
     * @param errorPrefix Prefix for the error.
     * @param traceId Unique identifier of this exception.
     * @param groupName Group name.
     * @param code Full error code.
     * @param message Original message.
     * @return New error message with predefined prefix.
     */
    static String errorMessage(String errorPrefix, UUID traceId, String groupName, int code, String message) {
        return errorPrefix + "-" + groupName + '-' + Short.toUnsignedInt(extractErrorCode(code))
                + ((message != null && !message.isEmpty()) ? ' ' + message : "") + " TraceId:" + traceId.toString().substring(0, 8);
    }
}
