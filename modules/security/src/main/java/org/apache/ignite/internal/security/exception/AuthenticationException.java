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

package org.apache.ignite.internal.security.exception;

import java.util.UUID;
import org.apache.ignite.lang.ErrorGroups.Authentication;
import org.apache.ignite.lang.IgniteException;

/**
 * The general authentication exception.
 */
public class AuthenticationException extends IgniteException {
    public AuthenticationException(String message) {
        super(Authentication.COMMON_AUTHENTICATION_ERR, message);
    }

    /**
     * Creates an exception with the given trace ID, error code, detailed message, and cause.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     * @param message Detailed message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public AuthenticationException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
