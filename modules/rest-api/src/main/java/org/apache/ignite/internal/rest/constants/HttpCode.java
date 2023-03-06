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

package org.apache.ignite.internal.rest.constants;

/**
 * Represents http codes that can be returned by Ignite.
 */
public enum HttpCode {
    OK(200, "OK"),
    BAD_REQUEST(400, "Bad Request"),
    UNAUTHORIZED(401, "Unauthorized"),
    FORBIDDEN(403, "Forbidden"),
    NOT_FOUND(404, "Not Found"),
    // May be used in case of "Already exists" problem.
    CONFLICT(409, "Conflict"),
    INTERNAL_ERROR(500, "Internal Server Error");

    private final int code;

    private final String message;

    HttpCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int code() {
        return code;
    }

    public String message() {
        return message;
    }

    /**
     * Create {@link HttpCode} from integer number.
     */
    public static HttpCode valueOf(int code) {
        switch (code) {
            case 200: return OK;
            case 400: return BAD_REQUEST;
            case 404: return NOT_FOUND;
            case 500: return INTERNAL_ERROR;
            default: throw new IllegalArgumentException(code + " is unknown http code");
        }
    }
}
